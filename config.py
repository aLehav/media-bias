from pathlib import Path

project_path = Path(r"C:\Users\adaml\OneDrive\Antisemitism")
data_path = project_path / "data"
colleges_dfs_path = data_path / "colleges_dfs"
queues_path = data_path / "queues"
screenshots_path = data_path / "screenshots"

gcs_api_request = {
    "params": {
        "key": "AIzaSyDJoVoRdPQBE6A4ZEjuJUDQodVR05G_gig",
        "cx": "0780d3804b33341ed",
        "safe": "off",
        "lr": "lang_en"
    },
    "num_newspaper_results": 1,
    "num_archive_results": 1
}
    
