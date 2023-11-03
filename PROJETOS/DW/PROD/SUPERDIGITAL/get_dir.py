from os import path
import os
from pathlib import Path


def get_onedrive_dirs() -> dict:

    onedrive = os.environ['ONEDRIVE']
    plan_dir = os.path.join(onedrive, "Documents - Planejamento")
    eng_dir = os.path.join(onedrive, "DataOffice", "CODIGOS", "PRODUCAO", "ENGENHARIA")
    dump_dir = os.path.join(onedrive, eng_dir, "ETL", "DUMP")
    log_dir = os.path.join(onedrive, eng_dir, "ETL", "LOGS")

    d = dict()
    d["onedrive"] = onedrive
    d["plan_dir"] = plan_dir
    d["eng_dir"] = eng_dir
    d["dump_dir"] = dump_dir
    d["log_dir"] = log_dir

    return d

