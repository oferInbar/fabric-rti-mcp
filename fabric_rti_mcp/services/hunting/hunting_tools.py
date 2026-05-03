from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from fabric_rti_mcp.services.hunting import hunting_service


def register_tools(mcp: FastMCP) -> None:
    """Register Advanced Hunting tools with the MCP server."""

    mcp.add_tool(
        hunting_service.run_hunting_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
