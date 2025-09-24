import json
from dagon import Workflow
from dagon.kubernetes_task import KubernetesTask

if __name__ == '__main__':
    workflow = Workflow("Taskflow-Demo-K8s")

    taskA = KubernetesTask("Tokio", "/bin/hostname")
    taskB = KubernetesTask("Berlin", "/bin/date")
    taskC = KubernetesTask("Nairobi", "/usr/bin/uptime")
    taskD = KubernetesTask("Mosco", "/bin/uname -a")

    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    taskB.add_dependency_to(taskA)
    taskC.add_dependency_to(taskA)
    taskD.add_dependency_to(taskB)
    taskD.add_dependency_to(taskC)

    jsonWorkflow = workflow.as_json()
    with open('taskflow-demo-k8s.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    workflow.run()
