from datetime import datetime
from pathlib import Path

from openhexa.sdk import current_run, parameter, pipeline, workspace
from utils import (
    get_dataset_version_timestamp,
    read_json_file,
    save_json_file,
    get_file_from_dataset,
    get_matching_filenames_from_dataset,
    push_data_to_db_table,
    save_to_parquet,
)


@pipeline("senes_table_update")
@parameter(
    "force_run",
    name="Force run",
    help="Force the pipeline to run even if no new data is detected.",
    type=bool,
    default=False,
    required=False,
)
def senes_table_update(force_run: bool):
    """Checks if new data is available and runs the update tasks if needed or if forced."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "senes_table_update"
    dataset_id = "senes-dataset"
    update_data = should_import_data(pipeline_path=pipeline_path, dataset_id=dataset_id) or force_run

    if update_data:
        # Run SENES table update
        run_senes_table_update(pipeline_path=pipeline_path, dataset_id=dataset_id, senes_db_table="COD_SENES")

        # Update last run timestamp
        update_last_run_timestamp(
            timestamp_filename=pipeline_path / "config" / "last_update.json",
            dataset_id=dataset_id,
        )
    else:
        current_run.log_info("No update needed. Skipping SENES table update.")


def should_import_data(pipeline_path: Path, dataset_id: str) -> bool:
    """Check if new data is available by comparing the latest dataset version timestamp.

    Returns:
        bool: True if an update is needed, False if data is up to date or on error.
    """
    try:
        new_version_dt = get_dataset_version_timestamp(dataset_id=dataset_id)
    except Exception as e:
        current_run.log_error(f"{e}")
        return False

    # read last run timestamp from file
    try:
        last_update = read_json_file(pipeline_path / "config" / "last_update.json")
        last_update_str = last_update.get("LAST_UPDATE", "")
        last_update_dt = datetime.strptime(last_update_str, "%Y%m%d_%H%M") if last_update_str else None
    except Exception as e:
        current_run.log_warning(f"Last update timestamp not found: Running update by default. Error: {e}")
        return True  # If we can't read the last update, assume we need to update

    if not last_update_dt or new_version_dt > last_update_dt:
        current_run.log_info("New data version detected. Update needed.")
        return True

    current_run.log_info("Data is up to date. No update needed.")
    return False


def run_senes_table_update(pipeline_path: Path, dataset_id: str, senes_db_table: str = "COD_SENES") -> None:
    """Placeholder function to run the actual SENES table update tasks."""
    current_run.log_info("Running SENES table update tasks...")

    senes_filenames = get_matching_filenames_from_dataset(
        dataset_id=dataset_id, pattern="senes_data_*.parquet"
    )
    if not senes_filenames:
        current_run.log_error("No SENES data file found in dataset.")
        return

    senes_filename = senes_filenames[0]
    senes_data = get_file_from_dataset(dataset_id, senes_filename)
    save_to_parquet(senes_data, pipeline_path / "data" / senes_filename)
    push_data_to_db_table(table_name=senes_db_table, dataframe=senes_data)
    current_run.log_info("SENES table update completed.")


def update_last_run_timestamp(timestamp_filename: Path, dataset_id: str) -> None:
    """Updates the last run timestamp in the JSON file."""
    timestamp = get_dataset_version_timestamp(dataset_id=dataset_id)
    try:
        save_json_file(
            file_path=timestamp_filename,
            contents={"LAST_UPDATE": timestamp.strftime("%Y%m%d_%H%M")},
        )
    except Exception as e:
        current_run.log_error(f"Error updating last run timestamp: {e}")


if __name__ == "__main__":
    senes_table_update()
