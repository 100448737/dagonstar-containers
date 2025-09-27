import os
import logging
import subprocess
import tempfile
import shutil
from dagon import Batch
from dagon.task import Task
import time
import json
import uuid
import threading

class ApptainerTask(Batch):
    """
    Representa una tarea que se ejecuta dentro de un contenedor Apptainer en HPC.
    Hereda de Batch para integrarse con el flujo de tareas de Dagon.
    """

    def __init__(self, name, command, image="docker://ubuntu:20.04", 
                 working_dir=None, remove=True, transversal_workflow=None,
                 bind_paths=None, overlay_size="1024", tmp_dir=None):
        """
        Inicializa la tarea de Apptainer.
        """
        Task.__init__(self, name, command, working_dir=working_dir,
                      transversal_workflow=transversal_workflow)

        self.image = image
        self.remove = remove
        self.bind_paths = bind_paths or []
        self.overlay_size = overlay_size
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        
        # Archivos y directorios del contenedor
        self.container_id = None
        self.sif_file = None
        self.overlay_file = None
        self.work_dir = None
        self.container_work_dir = "/work"
        
        # Directorio para staging de archivos entre contenedores
        self.staging_dir = None
        
        # Información del contenedor que Dagon necesita para staging
        self.info = None
        
        # Control de ejecución única
        self.executed = False
        self.execution_result = None
        
        # Lock para operaciones thread-safe
        self._lock = threading.Lock()

    def _run_apptainer_command(self, cmd_args, capture_output=True, check=True):
        """
        Ejecuta un comando de Apptainer con manejo de errores.
        """
        try:
            result = subprocess.run(
                cmd_args, 
                capture_output=capture_output, 
                text=True, 
                check=check,
                timeout=300
            )
            return result
        except subprocess.CalledProcessError as e:
            print(f"Error ejecutando Apptainer: {' '.join(cmd_args)}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            raise
        except subprocess.TimeoutExpired as e:
            print(f"Timeout ejecutando Apptainer: {' '.join(cmd_args)}")
            raise

    def create_container(self):
        """
        Prepara el entorno del contenedor Apptainer:
        - Descarga/construye la imagen SIF si es necesario
        - Crea overlay para escritura
        - Prepara directorios de trabajo y staging
        """
        if self.container_id is not None:
            print(f"Reutilizando contenedor existente: {self.container_id}")
            return

        with self._lock:
            if self.container_id is not None:
                return
            
            # Generar identificador único
            self.container_id = f"{self.name.lower()}-{uuid.uuid4().hex[:8]}-{int(time.time()*1000)}"
            
            # Crear directorio de trabajo temporal
            self.work_dir = os.path.join(self.tmp_dir, f"apptainer_work_{self.container_id}")
            os.makedirs(self.work_dir, exist_ok=True)
            
            # Crear directorio de staging para intercambio de archivos
            self.staging_dir = os.path.join(self.work_dir, "staging")
            os.makedirs(self.staging_dir, exist_ok=True)
            
            print(f"Preparando contenedor Apptainer: {self.container_id}")
            
            # Preparar imagen SIF
            self._prepare_sif_image()
            
            # Crear overlay para permitir escritura
            self._create_overlay()
            
            # Configurar información del contenedor
            self.info = {
                'name': self.name,
                'container_id': self.container_id,
                'work_dir': self.work_dir,
                'staging_dir': self.staging_dir,
                'sif_file': self.sif_file,
                'overlay_file': self.overlay_file
            }
            
            print(f"Contenedor {self.container_id} preparado exitosamente")

    def _prepare_sif_image(self):
        """
        Prepara la imagen SIF desde diferentes fuentes.
        """
        if self.image.endswith('.sif'):
            if os.path.exists(self.image):
                self.sif_file = self.image
                print(f"Usando archivo SIF existente: {self.sif_file}")
            else:
                raise FileNotFoundError(f"Archivo SIF no encontrado: {self.image}")
        else:
            self.sif_file = os.path.join(self.work_dir, f"{self.name}.sif")
            print(f"Construyendo imagen SIF desde: {self.image}")
            build_cmd = ["apptainer", "build", self.sif_file, self.image]
            try:
                result = self._run_apptainer_command(build_cmd, capture_output=False)
                print(f"Imagen SIF construida: {self.sif_file}")
            except subprocess.CalledProcessError:
                print("Reintentando construcción con sudo...")
                build_cmd.insert(0, "sudo")
                self._run_apptainer_command(build_cmd, capture_output=False)

    def _create_overlay(self):
        """
        Crea un overlay temporal para permitir escritura en el contenedor.
        """
        self.overlay_file = os.path.join(self.work_dir, f"overlay_{self.container_id}.img")
        print(f"Creando overlay de {self.overlay_size}MB...")
        create_cmd = [
            "apptainer", "overlay", "create", 
            "--size", self.overlay_size, 
            self.overlay_file
        ]
        self._run_apptainer_command(create_cmd)
        print(f"Overlay creado: {self.overlay_file}")

    def exec_in_container(self, command):
        """
        Ejecuta un comando dentro del contenedor Apptainer.
        """
        if not command.startswith(("mkdir -p", "cat > /tmp")):
            print(f"Ejecutando en contenedor: {command}")
        
        # Construir comando de ejecución
        exec_cmd = ["apptainer", "exec"]
        
        # Agregar overlay
        exec_cmd.extend(["--overlay", self.overlay_file])
        
        # Agregar bind paths
        for bind_path in self.bind_paths:
            exec_cmd.extend(["--bind", bind_path])
        
        # Bind del directorio de trabajo y staging
        exec_cmd.extend(["--bind", f"{self.work_dir}:{self.container_work_dir}"])
        exec_cmd.extend(["--bind", f"{self.staging_dir}:/staging"])
        
        # Cambiar al directorio de trabajo dentro del contenedor
        exec_cmd.extend(["--pwd", self.container_work_dir])
        
        # Archivo SIF e comando
        exec_cmd.extend([self.sif_file, "bash", "-c", command])
        
        result = self._run_apptainer_command(exec_cmd)
        return result.stdout

    def export_file_to_staging(self, container_path, staging_filename):
        """
        Exporta un archivo del contenedor al área de staging SIN overlay.
        Esto evita los conflictos de bloqueo completamente.
        """
        staging_path = os.path.join(self.staging_dir, staging_filename)
        
        # Comando SIN overlay - solo bind mounts
        exec_cmd = [
            "apptainer", "exec",
            "--bind", f"{self.work_dir}:{self.container_work_dir}",
            "--bind", f"{self.staging_dir}:/staging",
            "--pwd", self.container_work_dir,
            self.sif_file,
            "bash", "-c", f"cp {container_path} /staging/{staging_filename}"
        ]
        
        # Agregar bind paths adicionales si los hay
        for bind_path in self.bind_paths:
            exec_cmd.insert(-4, "--bind")
            exec_cmd.insert(-4, bind_path)
        
        print(f"Exportando {container_path} a staging (sin overlay)")
        result = self._run_apptainer_command(exec_cmd)
        
        # Verificar que el archivo fue creado
        if not os.path.exists(staging_path):
            raise FileNotFoundError(f"No se pudo exportar {container_path} a staging")
        
        return staging_path

    def import_file_from_staging(self, staging_path, container_path):
        """
        Importa un archivo del área de staging del host al contenedor.
        """
        if not os.path.exists(staging_path):
            raise FileNotFoundError(f"Archivo de staging no encontrado: {staging_path}")
        
        # Obtener nombre del archivo en staging
        staging_filename = os.path.basename(staging_path)
        
        # Crear directorio destino si no existe
        dir_dst = "/".join(container_path.split("/")[:-1])
        if dir_dst:
            self.exec_in_container(f"mkdir -p {dir_dst}")
        
        # Importar archivo del staging al contenedor
        import_cmd = f"cp /staging/{staging_filename} {container_path}"
        self.exec_in_container(import_cmd)

    def stage_in(self, src_task, src_path, dst_path):
        """
        Copia un archivo desde otro contenedor a este contenedor usando filesystem staging.
        Esta función es llamada por el framework de Dagon.
        """
        # Asegurar que ambos contenedores estén preparados
        if not hasattr(src_task, 'container_id') or src_task.container_id is None:
            src_task.create_container()
        if self.container_id is None:
            self.create_container()
        
        print(f"Copiando archivo {src_path} desde {src_task.name} a {self.name}")
        
        try:
            # Generar nombre único para el archivo en staging
            staging_filename = f"{src_task.name}_{src_path.replace('/', '_')}_{int(time.time()*1000)}"
            
            # Exportar archivo del contenedor fuente al staging
            staging_path = src_task.export_file_to_staging(src_path, staging_filename)
            
            # Copiar archivo de staging a nuestro staging (para que esté disponible en nuestro bind mount)
            our_staging_path = os.path.join(self.staging_dir, staging_filename)
            shutil.copy2(staging_path, our_staging_path)
            
            # Importar archivo de nuestro staging al contenedor
            self.import_file_from_staging(our_staging_path, dst_path)
            
            # Limpiar archivos temporales de staging
            try:
                os.remove(staging_path)
                os.remove(our_staging_path)
            except OSError:
                pass  # No es crítico si no se pueden eliminar
            
            print(f"Archivo copiado exitosamente via filesystem staging")
            
        except Exception as e:
            print(f"Error en stage_in: {e}")
            raise

    def cleanup_container(self):
        """
        Limpia archivos y directorios temporales del contenedor.
        """
        if self.remove and self.work_dir and os.path.exists(self.work_dir):
            try:
                print(f"Limpiando directorio de trabajo: {self.work_dir}")
                shutil.rmtree(self.work_dir)
                print(f"Directorio {self.work_dir} eliminado")
            except Exception as e:
                print(f"Advertencia: No se pudo eliminar directorio {self.work_dir}: {e}")
        
        # Limpiar referencias
        self.container_id = None
        self.sif_file = None
        self.overlay_file = None
        self.work_dir = None
        self.staging_dir = None
        self.info = None

    def pre_process_command(self, command):
        """
        Sobrescribir el método de preprocesamiento para interceptar workflow:/// URLs
        antes de que Dagon trate de procesarlas.
        """
        # Crear el contenedor si no existe
        if self.container_id is None:
            self.create_container()
        
        # Procesar workflow:/// manualmente
        if "workflow:///" in command:
            import re
            # Encontrar todas las referencias workflow:///
            workflow_refs = re.findall(r'workflow:///([^/\s]+)/([^\s]+)', command)
            
            for task_name, file_path in workflow_refs:
                workflow_url = f"workflow:///{task_name}/{file_path}"
                
                # Buscar la tarea referenciada en el workflow
                src_task = None
                if hasattr(self, 'workflow') and self.workflow:
                    for task in self.workflow.tasks:
                        if task.name == task_name:
                            src_task = task
                            break
                
                if src_task:
                    # Asegurar que la tarea fuente tenga su contenedor preparado
                    if not hasattr(src_task, 'container_id') or src_task.container_id is None:
                        src_task.create_container()
                    
                    # Crear archivo temporal local para simular el comportamiento esperado
                    local_path = f"/tmp/{task_name}_{file_path.replace('/', '_')}"
                    
                    try:
                        # Copiar archivo usando nuestro método stage_in con filesystem staging
                        self.stage_in(src_task, file_path, local_path)
                        # Reemplazar la referencia workflow:// con la ruta local
                        command = command.replace(workflow_url, local_path)
                    except Exception as e:
                        print(f"Error procesando workflow reference {workflow_url}: {e}")
        
        return command

    def on_execute(self, script, script_name):
        """
        Método llamado al ejecutar la tarea:
        - Prepara el contenedor si no existe
        - Ejecuta el comando dentro del contenedor
        - Devuelve el resultado en formato JSON
        """
        # Control de ejecución única
        if self.executed:
            print(f"[{self.name}] Devolviendo resultado previo")
            return self.execution_result

        Task.on_execute(self, script, script_name)

        # Preparar contenedor si no existe
        if self.container_id is None:
            self.create_container()

        # Procesar comando para manejar workflow:/// referencias
        processed_command = self.pre_process_command(self.command)

        # Ejecutar comando en el contenedor
        try:
            result = self.exec_in_container(processed_command).strip()
        except Exception as e:
            print(f"Error ejecutando comando en contenedor: {e}")
            result = f"Error: {str(e)}"

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
        - Elimina archivos temporales si corresponde
        """
        self.cleanup_container()
        super(ApptainerTask, self).on_garbage()