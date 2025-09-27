#!/usr/bin/env python3
"""
Demo corregido de DataFlow usando Apptainer
"""

import json
import os.path
import sys
import time

from dagon import Workflow
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':
    print("Iniciando demo corregido con Ubuntu (que tiene bash)...")
    
    # Crear el workflow de orquestación
    workflow = Workflow("DataFlow-Demo-Apptainer-Fixed")
    
    # Task A: crea un directorio y guarda el hostname en un archivo
    taskA = DagonTask(TaskType.APPTAINER, "A",
                      "mkdir -p output && hostname > output/f1.txt && echo 'Tarea A completada' >> output/f1.txt",
                      image="docker://ubuntu:20.04",
                      overlay_size="256")

    # Task B: genera número aleatorio y concatena el resultado de A  
    taskB = DagonTask(TaskType.APPTAINER, "B",
                      "echo $RANDOM > f2.txt && cat workflow:///A/output/f1.txt >> f2.txt",
                      image="docker://ubuntu:20.04",
                      overlay_size="256")

    # Task C: hace lo mismo que B (simulando otra rama)
    taskC = DagonTask(TaskType.APPTAINER, "C", 
                      "echo $RANDOM > f2.txt && cat workflow:///A/output/f1.txt >> f2.txt",
                      image="docker://ubuntu:20.04",
                      overlay_size="256")

    # Task D: combina las salidas de B y C y muestra el resultado
    taskD = DagonTask(TaskType.APPTAINER, "D",
                      "cat workflow:///B/f2.txt >> f3.txt && cat workflow:///C/f2.txt >> f3.txt && echo '=== Contenido final de f3.txt ===' && cat f3.txt",
                      image="docker://ubuntu:20.04",
                      overlay_size="256")

    # Añadir tareas al workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    # Construir dependencias automáticamente
    workflow.make_dependencies()

    # Guardar el workflow como JSON
    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-apptainer-fixed.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    print("Ejecutando workflow con Ubuntu...")
    
    # Ejecutar el workflow
    start_time = time.time()
    workflow.run()
    end_time = time.time()

    print(f"Workflow completado exitosamente en {end_time - start_time:.1f} segundos")
    
    # Dar tiempo para que se completen las operaciones de limpieza
    time.sleep(3)
    
    # Terminar explícitamente
    sys.exit(0)