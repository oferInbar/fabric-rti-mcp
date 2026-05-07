from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from fabric_rti_mcp.services.hunting import hunting_enrichment, hunting_insights, hunting_service


def register_core_tools(mcp: FastMCP) -> None:
    """Register core Advanced Hunting tools (schema discovery, query execution, validation)."""
    mcp.add_tool(
        hunting_service.run_hunting_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_service.get_hunting_schema,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_service.get_table_schema,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_service.validate_hunting_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )


def register_enrichment_tools(mcp: FastMCP) -> None:
    """Register enrichment tools (insights, actions, summarization, follow-ups)."""
    mcp.add_tool(
        hunting_insights.get_device_insights,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_insights.get_user_insights,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_enrichment.analyze_hunting_results,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_enrichment.suggest_hunting_followups,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_enrichment.get_available_hunting_actions,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        hunting_enrichment.summarize_hunting_results,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )


def register_tools(mcp: FastMCP) -> None:
    """Register all Advanced Hunting tools (core + enrichment)."""
    register_core_tools(mcp)
    register_enrichment_tools(mcp)
