from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from jinja2 import BaseLoader, Environment, Undefined, make_logging_undefined

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger("mnyxls")

# Set up a way to log undefined variables in Jinja templates.
# This will log a warning when an undefined variable is encountered.
# Use `StrictUndefined` to raise an error if a variable is not defined in the template.
LoggingUndefined = make_logging_undefined(logger=logger, base=Undefined)

######################################################################
# MnyXlsJinjaStringEnvironment


class MnyXlsJinjaStringEnvironment(Environment):
    """Custom Jinja Environment for config templates."""

    def __init__(self) -> None:
        """Constructor."""
        super().__init__(loader=BaseLoader(), undefined=LoggingUndefined)


jinjaenv = MnyXlsJinjaStringEnvironment()

######################################################################
# Jinja helpers


def render_template_str(template_src: str, template_vars: Mapping[str, Any], one_line: bool = True) -> str:
    """Render given string template as an output template.

    Args:
        template_src (str): Jinja source to compile into a template.
        template_vars (Mapping[str, Any]): Template variables.
        one_line (bool, optional): True if result should be a single line. Defaults to True.

    Returns:
        str
    """
    template_src = jinjaenv.from_string(template_src).render(**template_vars)

    if one_line:
        # Remove newlines, tabs, strings of spaces, etc.
        template_src = re.sub(r"\s+", " ", template_src).strip()

    return template_src
