import inspect
import logging
import os
import site


def get_caller_info() -> str | None:
    """jank ass call stack inspection to get same logging format as loguru"""
    try:
        stack = inspect.stack()
        site_packages_path = site.getsitepackages()[0]
        for i in range(len(stack) - 1, 1, -1):
            filename = stack[i].filename
            if os.path.basename(filename) == "loggingmachine.py":
                # get our actual caller frame
                prev_frame = stack[i + 1]
                full_path = prev_frame.filename
                # ensure `/Users/username/...` stripped
                full_path = full_path.replace(site_packages_path, "")
                module_path = (
                    full_path.replace(os.getcwd() + os.sep, "")
                    .replace(os.sep, ".")
                    .lstrip(".")
                )
                module_name = module_path.rsplit(".", 1)[0]
                function_name = prev_frame.function
                line_no = prev_frame.lineno
                caller_info = f"{module_name}:{function_name}:{line_no}".rjust(40)
                return caller_info
    except Exception:
        return None


class CustomFormatter(logging.Formatter):
    def format(self, record):
        caller_info = get_caller_info()
        if caller_info is None:
            # if we fail to inspect stack, default to log_format
            # log_format = "%(filename)s.%(funcName)s:%(lineno)s - %(message)s"
            caller_info = f"{record.filename}:{record.funcName}:{record.lineno}".rjust(
                40
            )
        module_name, function_name, line_no = caller_info.split(":")
        record.name = module_name
        record.filename = function_name
        record.lineno = int(line_no)

        return super().format(record)


def apply_custom_logging_format():
    # Retrieve the existing Bittensor logger
    bittensor_logger = logging.getLogger(
        "bittensor"
    )  # Ensure this matches the logger name you are using
    # bittensor_logger.setLevel(logging.INFO)  # Set the logging level to INFO

    # Apply the custom formatter to each handler
    date_format = "%Y-%m-%d %H:%M:%S"
    custom_formatter = CustomFormatter(datefmt=date_format)
    for handler in bittensor_logger.handlers:
        handler.setFormatter(custom_formatter)
