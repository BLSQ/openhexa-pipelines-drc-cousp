import os
import pandas as pd
from datetime import datetime
import papermill as pm

from openhexa.sdk import current_run, pipeline, parameter, workspace


@pipeline("mpox-table-update", name="MPOX_table_update")
@parameter(
    "mpox_file",
    name="Fichier MPOX (.xlsx)",
    help="Veuillez indiquer le nom du fichier mpox pour executer la mise a jour (.xlsx)",
    type=str,   
    required=True,
)
@parameter(
    "mpox_update",
    name="Mise a jour du DB",
    help="Mettre a jour la base de donnees?",
    type=bool,
    default=False,
    required=True,
)
def mpox_table_update(mpox_file:str, mpox_update:bool):
    """Write your pipeline orchestration here.

    This Pipeline will only call 2 python scripts
    1) Validation of the mpox file format.
    2) Upload of the data into the COUSP DB.

    """

    if mpox_file == '':
        current_run.log_error("Veuillez entrer un nom de fichier valide.")
        return

    # Setup variables
    nb_checker_name = "monkeyPox_format_checker"
    nb_checker_path = f"{workspace.files_path}/pipelines/MPOX_table_update/code"
    nb_checker_output = f"{workspace.files_path}/pipelines/MPOX_table_update/papermill_outputs"
   
   # Set parameters
    parameters = {
        'nom_fichier' : mpox_file,
        'oh_logs' : True,
    }

    # Run file checks    
    exec = run_update_with(nb_name=nb_checker_name, nb_path=nb_checker_path, out_nb_path=nb_checker_output, parameters=parameters, exec=True) 

    if mpox_update:
        # Setup variables
        nb_uploader_name = "monkeyPox_LOADER"
        nb_uploader_path = f"{workspace.files_path}/pipelines/MPOX_table_update/code"
        nb_uploader_output = f"{workspace.files_path}/pipelines/MPOX_table_update/papermill_outputs"

        # Run database update
        run_update_with(nb_name=nb_uploader_name, nb_path=nb_uploader_path, out_nb_path=nb_uploader_output, parameters=parameters, exec=exec) 


@mpox_table_update.task
def run_update_with(nb_name:str, nb_path:str, out_nb_path:str, parameters:dict, exec:bool=True):
    """
    Execute scripts for check and update.
    
    """         
    nb_full_path = os.path.join(nb_path, f"{nb_name}.ipynb")        

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")   
    out_nb_fname = f"{nb_name}_OUTPUT_{execution_timestamp}.ipynb" 
    out_nb_full_path = os.path.join(out_nb_path, out_nb_fname)

    if exec:
        try:
            current_run.log_info(f"Execution du notebook: {nb_full_path} demarre")
            pm.execute_notebook(input_path = nb_full_path,
                                output_path = out_nb_full_path,
                                parameters=parameters)
            current_run.log_info(f"Execution du notebook: {nb_full_path} termine.")
            return True    

        except pm.exceptions.PapermillExecutionError as e:
            current_run.log_error(f"Execution du notebook: {nb_full_path} a echoue! Papermill error") 
            return False
        except Exception as e:
            current_run.log_error(f"Execution du notebook: {nb_full_path} a echoue! Other Error ocurred {e}") 
            return False
            
    else:
        current_run.log_warning(f"Notebook: {nb_full_path} ne peut pas etre execute.")   
        return False


if __name__ == "__main__":
    mpox_table_update()