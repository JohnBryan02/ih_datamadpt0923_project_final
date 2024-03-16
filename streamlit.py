import streamlit as st
import subprocess

def ejecutar_script():
    # Activar el entorno virtual
    proceso_activar = subprocess.Popen(["conda", "activate", "jupyter_env"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    salida_activar, error_activar = proceso_activar.communicate()
    if proceso_activar.returncode != 0:
        st.error(f"Error al activar el entorno virtual: {error_activar.decode('utf-8')}")
        return

    # Ejecutar el archivo .py
    proceso_ejecutar = subprocess.Popen(["python", "finalcode.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    salida_ejecutar, error_ejecutar = proceso_ejecutar.communicate()
    if proceso_ejecutar.returncode != 0:
        st.error(f"Error al ejecutar el script: {error_ejecutar.decode('utf-8')}")
    else:
        st.success("El script se ejecutó correctamente.")

# Interfaz de usuario
st.title("Genera el ranking y los mails con las métricas")

if st.button("Ejecutar"):
    ejecutar_script()