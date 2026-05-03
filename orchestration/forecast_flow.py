from __future__ import annotations
import os
import sys
from typing import Any
sys.dont_write_bytecode = True
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, get_run_logger, task
try:
    from prefect.tasks import exponential_backoff
except ImportError:
    try:
        from prefect import exponential_backoff
    except ImportError:
        exponential_backoff = None
from prediction.forecast_pipeline import run_forecast_pipeline
_RETRIES = 3
if exponential_backoff is not None:
    _RETRY_DELAY = exponential_backoff(backoff_factor=8)
else:
    _RETRY_DELAY = [8, 32, 128]
_TASK = {'retries': _RETRIES, 'retry_delay_seconds': _RETRY_DELAY, 'retry_jitter_factor': 0.2}

@task(name='forecast_train_infer_publish', **_TASK)
def task_forecast_pipeline() -> dict[str, Any]:
    return run_forecast_pipeline()

@flow(name='CropGuard Forecast Pipeline', retries=0)
def cropguard_forecast_pipeline() -> dict[str, Any]:
    log = get_run_logger()
    log.info('CropGuard: starting forecast pipeline (drift-gated train + infer + publish)')
    out = task_forecast_pipeline()
    log.info('Forecast pipeline finished (keys=%s)', list(out.keys()) if isinstance(out, dict) else type(out).__name__)
    return out
if __name__ == '__main__':
    cropguard_forecast_pipeline()
