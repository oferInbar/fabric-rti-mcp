from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from fabric_rti_mcp.services.schema_catalog import schema_catalog_service


def register_tools(mcp: FastMCP) -> None:
    """Register schema catalog tools if SCHEMA_CATALOG_PATH is configured."""
    if not schema_catalog_service.is_configured():
        return

    mcp.add_tool(
        schema_catalog_service.schema_list_packs,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        schema_catalog_service.schema_list_tables,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        schema_catalog_service.schema_get_table,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        schema_catalog_service.schema_search,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
