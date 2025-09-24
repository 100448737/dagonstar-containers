from dagon import Batch
from dagon.task import Task
from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.client.rest import ApiException
import time
import json
import uuid

class KubernetesTask(Batch):
    """
    Representa una tarea que se ejecuta dentro de un pod de Kubernetes.
    Hereda de Batch para integrarse con el flujo de tareas de Dagon.
    """

    def __init__(self, name, command, image="ubuntu:20.04", namespace="default",
                 working_dir=None, remove=False, transversal_workflow=None):
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

        # Se asigna cuando se crea el pod
        self.pod_name = None

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
            print(f"Pod creado: {self.pod_name} (Namespace: {self.namespace})")
        except Exception as e:
            print(f"Error creando pod {self.pod_name}: {e}")

        # Esperar a que el pod esté en estado 'Running'
        while True:
            pod = self.v1.read_namespaced_pod(name=self.pod_name, namespace=self.namespace)
            if pod.status.phase == "Running":
                break
            time.sleep(0.5)

    def exec_in_pod(self, command):
        """
        Ejecuta un comando dentro del contenedor principal del pod.
        
        Args:
            command (str): Comando a ejecutar.

        Returns:
            str: Salida del comando ejecutado.
        """
        print(f"Ejecutando comando: {command}")
        resp = stream(self.v1.connect_get_namespaced_pod_exec,
                      self.pod_name,
                      self.namespace,
                      command=["/bin/bash", "-c", command],
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      container="main")
        return resp

    def remove_pod(self):
        """
        Elimina el pod si `remove=True`, similar a `docker run --rm`.
        Protege contra pods inexistentes.
        """
        if self.remove and self.pod_name is not None:
            try:
                self.v1.delete_namespaced_pod(
                    name=self.pod_name,
                    namespace=self.namespace,
                    body=client.V1DeleteOptions()
                )
                print(f"Pod {self.pod_name} eliminado")
            except ApiException as e:
                if e.status != 404:
                    print(f"Error eliminando pod {self.pod_name}: {e}")

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

        # Ejecutar comando
        result = self.exec_in_pod(self.command).strip()

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
        super(KubernetesTask, self).on_garbage()
        self.remove_pod()
