#!/bin/bash

# actualizar sistema
sudo apt update

# entorno virtual
sudo apt install python3-venv
sudo apt install python3-pip
pip3 install -U pip
pip3 install -U setuptools
sudo apt-get install python-dev libpq-dev
python3 -m venv env
source env/bin/activate

# instalar dependencias
pip3 install -r requirements.txt

# Ejecutar scripts
python main.py
