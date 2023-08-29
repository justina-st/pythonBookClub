from pathlib import Path
from typing import Union, List

import gzip
import json
from datetime import datetime
import logging


def _format_log_entry(request: List[dict], response: List[dict]) -> dict:
    log_entry = {
        "request": request,
        "response": response,
        "datetime": datetime.utcnow().timestamp(),
    }

    return log_entry


def log_request(path: Union[Path, str], request: List[dict], response: List[dict]):
    log_entry = _format_log_entry(request, response)

    path.mkdir(exist_ok=True, parents=True)
    filename = path / f"{datetime.utcnow().timestamp()}.json.gzip"
    try:
        with gzip.open(filename, "w") as f:
            f.write(json.dumps(log_entry).encode("utf-8"))
    except Exception as e:
        logging.error(f"Failed to log request: {e}")
