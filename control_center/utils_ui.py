import streamlit as st
import subprocess
import os
import sys
import re
from collections import deque

def get_project_root():
    """Returns the absolute path to the project root (one level up)."""
    current = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(current)

def run_script_with_log_viewer(script_path, args=None, key_prefix="proc", btn_label=None):
    """
    Runs a python script as a subprocess and streams output to a Streamlit component.
    Includes smart progress bar parsing.
    """
    
    project_root = get_project_root()
    full_script_path = os.path.join(project_root, script_path)
    label = btn_label or f"🚀 Ejecutar {os.path.basename(script_path)}"
    
    if not os.path.exists(full_script_path):
        st.error(f"Script not found: {full_script_path}")
        return

    if st.button(label, key=f"{key_prefix}_btn", type="primary"):
        
        st.info(f"Iniciando proceso: {script_path}...")
        
        # UI Elements
        
        # Progress Bars
        col_glob, col_unit = st.columns(2)
        with col_glob:
            st.caption("Progreso Global (Lote)")
            prog_global = st.progress(0)
        with col_unit:
            st.caption("Progreso Unitario (Producto Actual)")
            prog_unit = st.progress(0)
            
        status_box = st.status("Ejecutando...", expanded=True)
        log_container = st.empty()
        
        # Prepare command
        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)
            
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        env["PYTHONUNBUFFERED"] = "1"

        try:
            process = subprocess.Popen(
                cmd,
                cwd=project_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            logs = deque(maxlen=1000)
            
            # Regex for progress [Current/Total]
            # Capture groups: 1=Current, 2=Total
            regex_pattern = re.compile(r"\[(\d+)/(\d+)\]")
            
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    clean_line = output.rstrip()
                    logs.append(clean_line)
                    
                    # Update Logs
                    log_text = "\n".join(list(logs)[-30:]) 
                    log_container.code(log_text, language="bash")
                    
                    # Analyze for Progress
                    match = regex_pattern.search(clean_line)
                    if match:
                        curr, total = int(match.group(1)), int(match.group(2))
                        if total > 0:
                            ratio = min(curr / total, 1.0)
                            
                            # Heuristic: 
                            # If Total > 5 -> Global Batch Progress
                            # If Total <= 5 -> Unit Step Progress
                            if total > 6: 
                                prog_global.progress(ratio)
                            else:
                                prog_unit.progress(ratio)
            
            return_code = process.poll()
            
            if return_code == 0:
                status_box.update(label="✅ Ejecución Completada", state="complete", expanded=False)
                prog_global.progress(1.0)
                prog_unit.progress(1.0)
                st.success(f"{script_path} finalizó exitosamente.")
            else:
                status_box.update(label="❌ Fallo en Ejecución", state="error", expanded=True)
                st.error(f"El proceso falló con código {return_code}")
                st.code("\n".join(list(logs)[-50:]))

        except Exception as e:
            status_box.update(label="❌ Error Crítico", state="error")
            st.error(f"Ocurrió un error: {e}")
