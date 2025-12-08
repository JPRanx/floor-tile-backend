"""
Excel and file parsers module.

See BUILDER_BLUEPRINT.md for parser specifications.
"""

from parsers.excel_parser import (
    parse_owner_excel,
    ExcelParseResult,
)

__all__ = [
    "parse_owner_excel",
    "ExcelParseResult",
]
