import json
import os.path
import sys
import time

from dagon import Workflow
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':
    # Crear el workflow de orquestación
    workflow = Workflow("DataFlow-Demo-K8s")

    # Task A: crea un directorio y guarda el hostname en un archivo
    taskA = DagonTask(TaskType.KUBERNETES, "A",
                      "mkdir output; hostname > output/f1.txt",
                      image="ubuntu:20.04")

    # Task B: genera número aleatorio y concatena el resultado de A
    taskB = DagonTask(TaskType.KUBERNETES, "B",
                      "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt",
                      image="python:3.9")

    # Task C: hace lo mismo que B (simulando otra rama)
    taskC = DagonTask(TaskType.KUBERNETES, "C",
                      "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt",
                      image="python:3.9")

    # Task D: combina las salidas de B y C y muestra el resultado
    taskD = DagonTask(TaskType.KUBERNETES, "D",
                  "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt; echo '=== Contenido final de f3.txt ==='; cat f3.txt",
                  image="centos:8")

    # Añadir tareas al workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    # Construir dependencias automáticamente
    workflow.make_dependencies()

    # Guardar el workflow como JSON
    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-k8s.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # Ejecutar el workflow
    workflow.run()

    print("Workflow completado exitosamente")
    print("El archivo f3.txt con los resultados finales está dentro del pod de la tarea D")
    
    # Dar tiempo para que se completen las operaciones de limpieza
    time.sleep(2)
    
    # Terminar explícitamente
    sys.exit(0)
