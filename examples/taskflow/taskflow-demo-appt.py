#!/usr/bin/env python3
"""
Demo de Taskflow usando ApptainerTask
"""

import json
import os.path
import sys
import time

from dagon import Workflow
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':
    print("Iniciando demo de Taskflow con Apptainer...")
    
    # Crear el workflow de orquestación
    workflow = Workflow("Taskflow-Demo-Apptainer")

    # Crear tareas equivalentes al demo de Kubernetes usando Ubuntu
    taskA = DagonTask(TaskType.APPTAINER, "Tokio", 
                     "/bin/hostname && echo 'Task Tokio completed'",
                     image="docker://ubuntu:20.04",
                     overlay_size="256")

    taskB = DagonTask(TaskType.APPTAINER, "Berlin", 
                     "/bin/date && echo 'Task Berlin completed'",
                     image="docker://ubuntu:20.04",
                     overlay_size="256")

    taskC = DagonTask(TaskType.APPTAINER, "Nairobi", 
                     "/usr/bin/uptime && echo 'Task Nairobi completed'",
                     image="docker://ubuntu:20.04",
                     overlay_size="256")

    taskD = DagonTask(TaskType.APPTAINER, "Mosco", 
                     "/bin/uname -a && echo 'Task Mosco completed'",
                     image="docker://ubuntu:20.04",
                     overlay_size="256")

    # Añadir tareas al workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    # Establecer dependencias
    taskB.add_dependency_to(taskA)
    taskC.add_dependency_to(taskA)
    taskD.add_dependency_to(taskB)
    taskD.add_dependency_to(taskC)

    # Guardar el workflow como JSON
    jsonWorkflow = workflow.as_json()
    with open('taskflow-demo-apptainer.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    print("Ejecutando workflow con Apptainer...")
    
    # Ejecutar el workflow
    start_time = time.time()
    workflow.run()
    end_time = time.time()

    print(f"Workflow completado exitosamente en {end_time - start_time:.1f} segundos")
    
    # Dar tiempo para que se completen las operaciones de limpieza
    time.sleep(3)
    
    # Terminar explícitamente
    sys.exit(0)