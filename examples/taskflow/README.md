# Kubernetes Tasks with DagOnStar

DagOnStar supports the deployment of tasks on Kubernetes clusters using [K3s](https://k3s.io/).

## Requirements

* [K3s](https://k3s.io/) (lightweight Kubernetes distribution)
* Python 3.8+
* Virtualenv

## Installation

Clone the repository and prepare the Python environment:

```bash
git clone https://github.com/CoCo-ARCOS/dagonstar-containers.git
cd dagonstar-containers/

virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD:$PYTHONPATH
```
Copy the configuration file:

```bash
cp dagon.ini.sample examples/taskflow/dagon.ini
```

Set up K3s automatically with the provided script:

```bash
bash dagon/setup-k3s.sh
```

This will:

Install K3s

Configure ~/.kube/config

Set the KUBECONFIG environment variable permanently

[!NOTE]
You only need to run this setup once.

## Running Kubernetes tasks

Navigate to the Kubernetes example and run the demo:

```bash
cd examples/taskflow/
python taskflow-demo-k8s.py
```
During the execution, the demo will create pods for each task, wait until they are running, execute the commands, and finally clean up the pods (if you specify remove=True).


