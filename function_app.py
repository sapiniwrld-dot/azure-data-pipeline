import logging
import os
import tempfile

os.environ.setdefault(
    "PIPELINE_DATA_DIR", os.path.join(tempfile.gettempdir(), "pipeline-data")
)

import azure.functions as func  # noqa: E402

import run_pipeline  # noqa: E402

app = func.FunctionApp()

logging.getLogger("azure").setLevel(logging.WARNING)


@app.timer_trigger(schedule="0 0 */6 * * *", arg_name="timer", run_on_startup=False)
def pipeline_timer(timer: func.TimerRequest) -> None:
    if run_pipeline.main() != 0:
        raise RuntimeError("Pipeline failed, see the log above")
