#!/usr/bin/env bash
set -e

echo "[Apptainer] Instalando..."

# Detectar e instalar según el sistema
if command -v apptainer &> /dev/null || command -v singularity &> /dev/null; then
    echo "[Apptainer] Ya está instalado ✅"
elif [ -f /etc/debian_version ]; then
    sudo apt update && sudo apt install -y software-properties-common
    sudo add-apt-repository -y ppa:apptainer/ppa && sudo apt update
    sudo apt install -y apptainer
elif [ -f /etc/redhat-release ]; then
    sudo dnf install -y epel-release apptainer 2>/dev/null || sudo yum install -y epel-release singularity
else
    # Instalación genérica
    curl -s https://get.apptainer.org | bash
fi

# Configurar directorios y variables
echo "[Apptainer] Configurando entorno..."
mkdir -p ~/.apptainer/{cache,tmp}

# Detectar comando disponible
APPTAINER_CMD="apptainer"
command -v apptainer &> /dev/null || APPTAINER_CMD="singularity"

# Configurar variables permanentemente
cat >> ~/.bashrc << EOF
# Apptainer para DagOnStar
export APPTAINER_CACHEDIR=\$HOME/.apptainer/cache
export APPTAINER_TMPDIR=\$HOME/.apptainer/tmp
export APPTAINER_CMD=$APPTAINER_CMD
EOF

# Exportar para sesión actual
export APPTAINER_CACHEDIR=$HOME/.apptainer/cache
export APPTAINER_TMPDIR=$HOME/.apptainer/tmp
export APPTAINER_CMD=$APPTAINER_CMD

# Instalar dependencias Python
pip install --quiet spython psutil filelock tqdm

# Test rápido
echo "[Apptainer] Probando funcionalidad..."
timeout 120 $APPTAINER_CMD exec docker://alpine:latest echo "Test exitoso" || echo "⚠️ Test falló (puede funcionar normalmente)"

echo "[Apptainer] Instalación completada ✅"
echo "Comando disponible: $APPTAINER_CMD"