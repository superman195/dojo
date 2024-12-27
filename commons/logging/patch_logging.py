import inspect
import os

from bittensor.utils.btlogging import logging


def custom_format(cls, prefix: object, suffix: object = None):
    try:
        frame = inspect.currentframe().f_back.f_back
        args, _, _, value_dict = inspect.getargvalues(frame)
        class_or_filename = None
        if len(args) and args[0] == "self":
            # for instance methods
            instance = value_dict.get("self", None)
            if instance:
                class_or_filename = instance.__class__.__name__
        elif len(args) and args[0] == "cls":
            # for class methods
            class_type = value_dict.get("cls", None)
            if class_type:
                class_or_filename = class_type.__name__
        else:
            # for staticmethods
            class_or_filename = frame.f_globals.get("__qualname__", "").split(".")[0]

        (
            filename,
            line_number,
            func_name,
            lines,
            index,
        ) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        # filename_no_ext, ext = os.path.splitext(filename)

        if not class_or_filename:
            class_or_filename = "[FILE] ".rjust(8) + filename
        else:
            class_or_filename = "[CLASS] ".rjust(8) + class_or_filename

        if func_name.startswith("<") or func_name.endswith(">"):
            func_name = "\\" + func_name
        context = f"<cyan>{class_or_filename}:{func_name}:{line_number}</cyan>".center(
            30
        )
        log_msg = f"{context} | {prefix}"
        if suffix is not None:
            log_msg += f" | {suffix}"
    except:  # noqa: E722
        log_msg = str(prefix).ljust(30) + str(suffix)

    return log_msg


# original_format = logging._format


def apply_patch():
    # monkey patch
    logging._format = classmethod(custom_format)
