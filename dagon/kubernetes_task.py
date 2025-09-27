import os
import logging
from dagon import Batch
from dagon.task import Task
from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.client.rest import ApiException
import time
import json
import uuid

# Reducir logs de Kubernetes
logging.getLogger('kubernetes.client.rest').setLevel(logging.WARNING)

class KubernetesTask(Batch):
    """
    Representa una tarea que se ejecuta dentro de un pod de Kubernetes.
    Hereda de Batch para integrarse con el flujo de tareas de Dagon.
    """

    def __init__(self, name, command, image="ubuntu:20.04", namespace="default",
                 working_dir=None, remove=True, transversal_workflow=None, cleanup_timeout=30):
        """
        Inicializa la tarea de Kubernetes.

        Args:
            name (str): Nombre de la tarea.
            command (str): Comando a ejecutar dentro del pod.
            image (str): Imagen de contenedor a usar.
            namespace (str): Namespace de Kubernetes.
            working_dir (str, opcional): Directorio de trabajo para la tarea.
            remove (bool): Si True, elimina el pod al finalizar.
            transversal_workflow: Flujo de trabajo transversal si aplica.
        """
        # Inicializar la tarea base de Dagon
        Task.__init__(self, name, command, working_dir=working_dir,
                      transversal_workflow=transversal_workflow)

        # Cargar la configuración de Kubernetes (desde ~/.kube/config)
        config.load_kube_config()

        # API para gestionar pods
        self.v1 = client.CoreV1Api()

        self.image = image
        self.namespace = namespace
        self.remove = remove
        self.cleanup_timeout = cleanup_timeout

        # Se asigna cuando se crea el pod
        self.pod_name = None
        
        # Información del pod que Dagon necesita para staging
        self.info = None

        # Control de ejecución única
        self.executed = False
        self.execution_result = None

    def create_pod(self):
        """
        Crea un pod en Kubernetes solo si no existe ya (evita duplicados).

        - El pod se mantiene en estado 'sleep infinity' para permitir múltiples ejecuciones.
        """
        if self.pod_name is not None:
            # Ya existe un pod creado, lo reutilizamos
            print(f"Reutilizando pod existente: {self.pod_name}")
            return

        # Generar un nombre único usando UUID y timestamp
        self.pod_name = f"{self.name.lower()}-{uuid.uuid4().hex[:8]}-{int(time.time()*1000)}"

        # Definición del pod
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": self.pod_name, "labels": {"app": self.name}},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": self.image,
                    "command": ["/bin/bash", "-c", "sleep infinity"],
                }],
                "restartPolicy": "Never"
            }
        }

        try:
            self.v1.create_namespaced_pod(namespace=self.namespace, body=pod_manifest)
            print(f"Pod creado: {self.pod_name}")
        except Exception as e:
            print(f"Error creando pod {self.pod_name}: {e}")

        # Esperar a que el pod esté en estado 'Running' y obtener IP
        print(f"Esperando a que el pod {self.pod_name} esté listo...")
        while True:
            pod = self.v1.read_namespaced_pod(name=self.pod_name, namespace=self.namespace)
            if pod.status.phase == "Running":
                pod_ip = pod.status.pod_ip
                print(f"Pod {self.pod_name} listo con IP: {pod_ip}")
                
                # Configurar información del pod que Dagon necesita
                self.info = {
                    'name': self.name,
                    'ip': pod_ip,
                    'pod_name': self.pod_name,
                    'namespace': self.namespace
                }
                break
            elif pod.status.phase == "Failed":
                raise Exception(f"Pod {self.pod_name} falló: {pod.status.message}")
            time.sleep(0.5)

    def exec_in_pod(self, command):
        """
        Ejecuta un comando dentro del contenedor principal del pod.
        Args:
            command (str): Comando a ejecutar.

        Returns:
            str: Salida del comando ejecutado.
        """
        # Reducir logging solo mostrar comandos importantes
        if not command.startswith(("mkdir -p", "cat > /tmp")):
            print(f"Ejecutando: {command}")
        resp = stream(self.v1.connect_get_namespaced_pod_exec,
                      self.pod_name,
                      self.namespace,
                      command=["/bin/bash", "-c", command],
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      container="main")
        return resp

    def stage_in(self, src_task, src_path, dst_path):
        """
        Copia un archivo desde otro pod a este pod para que workflow:/// funcione.
        Esta función es llamada por el framework de Dagon.
        """
        # Asegurar que ambos pods estén creados y con información disponible
        if not hasattr(src_task, 'pod_name') or src_task.pod_name is None:
            src_task.create_pod()
        if self.pod_name is None:
            self.create_pod()
            
        print(f"Copiando archivo {src_path} desde {src_task.name} a {self.name}")
        
        try:
            # Leer contenido del archivo en el pod fuente
            content = src_task.exec_in_pod(f"cat {src_path}")

            # Crear carpeta de destino si no existe
            dir_dst = "/".join(dst_path.split("/")[:-1])
            if dir_dst:
                self.exec_in_pod(f"mkdir -p {dir_dst}")

            # Escribir contenido en destino usando heredoc para evitar problemas con caracteres especiales
            escaped_content = content.replace("'", "'\"'\"'")
            self.exec_in_pod(f"cat > {dst_path} << 'EOF'\n{escaped_content}\nEOF")
            print(f"Archivo copiado exitosamente")
        except Exception as e:
            print(f"Error en stage_in: {e}")
            raise

    def remove_pod(self):
        """
        Elimina el pod si `remove=True`, similar a `docker run --rm`.
        Protege contra pods inexistentes.
        """
        if self.remove and self.pod_name is not None:
            pod_to_delete = self.pod_name  # Guardar referencia antes de limpiar
            try:
                # Método 1: Eliminación estándar primero
                try:
                    self.v1.delete_namespaced_pod(
                        name=pod_to_delete,
                        namespace=self.namespace,
                        body=client.V1DeleteOptions(grace_period_seconds=30)
                    )
                    print(f"Pod {pod_to_delete} eliminado")
                except ApiException as e:
                    if e.status == 404:
                        print(f"Pod {pod_to_delete} ya no existe")
                    else:
                        # Método 2: Forzar eliminación inmediata si falla el método estándar
                        print(f"Eliminación estándar falló, forzando eliminación de {pod_to_delete}")
                        self.v1.delete_namespaced_pod(
                            name=pod_to_delete,
                            namespace=self.namespace,
                            body=client.V1DeleteOptions(
                                grace_period_seconds=0,
                                propagation_policy='Background'
                            )
                        )
                        print(f"Pod {pod_to_delete} eliminado forzadamente")
                        
            except ApiException as e:
                if e.status != 404:
                    print(f"Advertencia: No se pudo eliminar pod {pod_to_delete}: {e.reason}")
                    # Método 3: Última opción - usar kubectl si la API falla
                    try:
                        import subprocess
                        result = subprocess.run(
                            ['kubectl', 'delete', 'pod', pod_to_delete, '--force', '--grace-period=0'],
                            capture_output=True, text=True, timeout=10
                        )
                        if result.returncode == 0:
                            print(f"Pod {pod_to_delete} eliminado usando kubectl")
                        else:
                            print(f"kubectl también falló para {pod_to_delete}: {result.stderr}")
                    except Exception as kubectl_error:
                        print(f"kubectl no disponible para limpiar {pod_to_delete}: {kubectl_error}")
            except Exception as e:
                print(f"Error inesperado eliminando pod {pod_to_delete}: {e}")
            finally:
                # Limpiar referencias independientemente del resultado
                self.pod_name = None
                self.info = None
    def pre_process_command(self, command):
        """
        Sobrescribir el método de preprocesamiento para interceptar workflow:/// URLs
        antes de que Dagon trate de procesarlas.
        """
        # Crear el pod si no existe para asegurarse de que tenemos información disponible
        if self.pod_name is None:
            self.create_pod()
        
        # Procesar workflow:/// manualmente para evitar el error de KeyError
        if "workflow:///" in command:
            import re
            
            # Encontrar todas las referencias workflow:///
            workflow_refs = re.findall(r'workflow:///([^/\s]+)/([^\s]+)', command)
            
            for task_name, file_path in workflow_refs:
                # Buscar la tarea referenciada en el workflow
                src_task = None
                if hasattr(self, 'workflow') and self.workflow:
                    for task in self.workflow.tasks:
                        if task.name == task_name:
                            src_task = task
                            break
                
                if src_task:
                    # Asegurar que la tarea fuente tenga su pod creado
                    if not hasattr(src_task, 'pod_name') or src_task.pod_name is None:
                        src_task.create_pod()
                    
                    # Crear archivo temporal local para simular el comportamiento esperado
                    local_path = f"/tmp/{task_name}_{file_path.replace('/', '_')}"
                    
                    try:
                        # Copiar archivo usando nuestro método stage_in
                        self.stage_in(src_task, file_path, local_path)
                        
                        # Reemplazar la referencia workflow:// con la ruta local
                        workflow_url = f"workflow:///{task_name}/{file_path}"
                        command = command.replace(workflow_url, local_path)
                    except Exception as e:
                        print(f"Error procesando workflow reference {workflow_url}: {e}")
        
        return command

    def on_execute(self, script, script_name):
        """
        Método llamado al ejecutar la tarea:

        - Crea el pod si no existe.
        - Ejecuta el comando dentro del pod.
        - Devuelve el resultado en formato JSON escapando saltos de línea y tabs.
        """
        # Control de ejecución única
        if self.executed:
            print(f"[{self.name}] Devolviendo resultado previo")
            return self.execution_result

        Task.on_execute(self, script, script_name)

        # Crear pod si no existe
        if self.pod_name is None:
            self.create_pod()

        # Procesar comando para manejar workflow:/// referencias
        processed_command = self.pre_process_command(self.command)

        # Ejecutar comando
        result = self.exec_in_pod(processed_command).strip()

        # Escapar saltos de línea y tabs
        safe_result = result.replace("\n", "\\n").replace("\t", "\\t")

        # Formatear salida como JSON
        output_json = json.dumps({"result": safe_result})
        print(f"[{self.name}] Output:\n{output_json}")

        # Marcar como ejecutado y guardar resultado
        self.executed = True
        self.execution_result = {"output": output_json, "code": 0}
        return self.execution_result

    def on_garbage(self):
        """
        Se llama al limpiar la tarea:

        - Elimina el pod si corresponde.
        """
        self.remove_pod()
        super(KubernetesTask, self).on_garbage()