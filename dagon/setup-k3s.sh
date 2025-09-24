#!/usr/bin/env bash
set -e

echo "[K3s] Instalando..."
curl -sfL https://get.k3s.io | sh -

echo "[K3s] Configurando kubeconfig en ~/.kube/config..."
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config

# Export para la sesión actual
export KUBECONFIG=$HOME/.kube/config
echo "export KUBECONFIG=$HOME/.kube/config" >> ~/.bashrc

echo "[K3s] Instalación completada ✅"
