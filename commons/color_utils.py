"""
Utilities for handling color tags in log messages.
Converts tags like [red], [blue], etc. to ANSI color codes.
"""

import logging
import re
from typing import Match

# ANSI color code constants
ANSI_COLORS = {
    "black": "\033[30m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "white": "\033[37m",
    "reset": "\033[0m",
    # Bold/bright variants
    "bright_black": "\033[90m",
    "bright_red": "\033[91m",
    "bright_green": "\033[92m",
    "bright_yellow": "\033[93m",
    "bright_blue": "\033[94m",
    "bright_magenta": "\033[95m",
    "bright_cyan": "\033[96m",
    "bright_white": "\033[97m",
}


def convert_tags_to_ansi(text: str) -> str:
    """
    Convert color tags in text to ANSI color codes.

    Example:
        "[magenta]Hello[/magenta] [blue]World[/blue]" becomes
        "\033[35mHello\033[0m \033[34mWorld\033[0m"

    Args:
        text: Text containing color tags

    Returns:
        Text with ANSI color codes
    """
    # Pattern to match color tags: [color] or [/color]
    pattern = r"\[(/?[a-zA-Z_]+)\]"

    def replace_tag(match: Match[str]) -> str:
        tag = match.group(1)
        if tag.startswith("/"):
            # Closing tag, reset the color
            return ANSI_COLORS["reset"]
        elif tag in ANSI_COLORS:
            # Opening tag with known color
            return ANSI_COLORS[tag]
        else:
            # Unknown tag, return as is
            return match.group(0)

    # Replace all color tags with their ANSI equivalents
    return re.sub(pattern, replace_tag, text)


class ColorTagsFilter(logging.Filter):
    """
    Logging filter that converts color tags to ANSI escape sequences.

    Usage:
        logger = logging.getLogger('your_logger')
        logger.addFilter(ColorTagsFilter())
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, "msg") and isinstance(record.msg, str):
            record.msg = convert_tags_to_ansi(record.msg)
        return True


# Convenience function to install the filter on a logger
def install_color_tags_filter(logger: logging.Logger) -> None:
    """
    Install a color tags filter on the given logger.

    Args:
        logger: Logger instance to install the filter on
    """
    logger.addFilter(ColorTagsFilter())
