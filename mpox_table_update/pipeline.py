from datetime import datetime
from pathlib import Path

import papermill as pm
from openhexa.sdk import current_run, parameter, pipeline, workspace, File
from validation import XLSFormValidator


@pipeline("mpox-table-update")
@parameter(
    code="mpox_file",
    name="Fichier MPOX (.xlsx)",
    type=File,
    help="Veuillez indiquer le fichier mpox pour executer la mise a jour (.xlsx)",
    required=True,
)
@parameter(
    code="mpox_update",
    name="Mise a jour du DB",
    help="Mettre a jour la base de donnees COUSP avec les donnees du fichier mpox",
    type=bool,
    default=False,
    required=True,
)
def mpox_table_update(mpox_file: File, mpox_update: bool):
    """Write your pipeline orchestration here.

    This Pipeline will perform two steps:
    1) Validation/check of the mpox file format using XSLForm validation.py.
    2) Upload of the data into the COUSP DB.

    """
    # Setup variables
    pipeline_path = Path(workspace.files_path) / "pipelines" / "MPOX_table_update"
    source_folder = Path(workspace.files_path) / "monkeyPox" / "Data" / "LL"  # default folder for mpox files

    # Only load data from this folder
    if not (source_folder / mpox_file.name).exists():
        current_run.log_error(
            f"Le fichier mpox ne peut pas être trouvé dans : monkeyPox/Data/LL/{mpox_file.name}"
        )
        raise FileNotFoundError

    # Set parameters
    parameters = {
        "nom_fichier": mpox_file.name,
    }

    # Run excel validation
    run_validation(
        xlsform_path=pipeline_path / "validation" / "MPOX_LL_validation.xlsx",
        excelfile_path=source_folder / mpox_file.path,
        results=pipeline_path / "validation" / "results",
    )

    if mpox_update:
        # Run database update
        run_update_with(
            nb_path=pipeline_path / "code" / "monkeyPox_LOADER.ipynb",
            out_nb_path=pipeline_path / "papermill_outputs",
            parameters=parameters,
        )
    current_run.log_info("Pipeline execution completed.")


def run_update_with(nb_path: Path, out_nb_path: Path, parameters: dict):
    """Execute scripts for check and update."""
    execution_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_nb_path = out_nb_path / f"{nb_path.stem}_OUTPUT_{execution_timestamp}.ipynb"

    try:
        current_run.log_info(f"Execution du notebook: {nb_path} demarre")
        pm.execute_notebook(input_path=nb_path, output_path=out_nb_path, parameters=parameters)
        current_run.log_info(f"Execution du notebook: {nb_path} termine.")

    except pm.exceptions.PapermillExecutionError:
        current_run.log_error(f"Execution du notebook: {nb_path} a echoue! (Papermill error)")
    except Exception as e:
        current_run.log_error(f"Execution du notebook: {nb_path} a echoue! Other Error ocurred {e}")


def run_validation(xlsform_path: Path, excelfile_path: Path, results: Path):
    """Run the XLSForm validation on the provided Excel file."""
    current_run.log_info("Starting Excel validation...")
    result_path = results / f"{excelfile_path.stem}_result{excelfile_path.suffix}"
    results.mkdir(parents=True, exist_ok=True)
    validator = XLSFormValidator()

    if not xlsform_path.exists():
        current_run.log_error(f"Fichier XLSForm introuvable : {xlsform_path}")
        raise FileNotFoundError

    if not excelfile_path.exists():
        current_run.log_error(f"Fichier Excel Mpox d'entrée introuvable {excelfile_path}")
        raise FileNotFoundError

    if excelfile_path.suffix != ".xlsx":
        current_run.log_error(
            f"Le fichier doit être un fichier Excel (.xlsx), trouvé : {excelfile_path.name}"
        )
        raise ValueError

    try:
        parse_xslform_with(xlsform_path, validator)
        # current_run.log_info(f"XLSForm correctly loaded {xlsform_path}")
    except Exception:
        current_run.log_error(
            f"Erreur lors de l'analyse du formulaire XLSForm {xlsform_path}. "
            "La validation ne peut pas être effectuée."
        )
        raise

    current_run.log_info(f"Validation du fichier Excel {excelfile_path}")
    with Path.open(excelfile_path, "rb") as spreadsheet_file:
        validation_result = validator.validate_spreadsheet(spreadsheet_file)

    if validation_result.get("is_valid"):
        current_run.log_info("Résultat de la validation Excel : VALIDE")
        return

    current_run.log_warning("Résultat de la validation Excel : INVALIDE")

    # Invalid case
    errors = validation_result.get("errors", [])
    # output_payload = {"result": "invalid", "errors": errors}

    with Path.open(excelfile_path, "rb") as spreadsheet_file:
        highlighted_buffer = validator.create_highlighted_excel(spreadsheet_file, errors)
    with Path.open(result_path, "wb") as out_f:
        out_f.write(highlighted_buffer.read())

    # Log the result file as output
    current_run.add_file_output(result_path.as_posix())
    current_run.log_info(f"Le fichier révisé a été enregistré dans le dossier suivant : {result_path}")
    current_run.log_warning(
        "Veuillez vérifier les résultats de la validation du "
        "fichier Excel pour corriger d'éventuelles erreurs."
    )


def parse_xslform_with(xlsform_path: Path, validator: XLSFormValidator):
    """Parse the XLSForm to ensure it is valid."""
    with Path.open(xlsform_path, "rb") as xlsform_file:
        parsed = validator.parse_xlsform(xlsform_file)
        if not parsed:
            raise ValueError("Failed to parse XLSForm file.")


if __name__ == "__main__":
    mpox_table_update()
