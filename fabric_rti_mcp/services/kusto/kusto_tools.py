from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from fabric_rti_mcp.services.kusto import kusto_service
from fabric_rti_mcp.services.kusto.kusto_service import CONFIG
from fabric_rti_mcp.services.kusto.shots_table_ref import ShotsTableRef


def register_tools(mcp: FastMCP) -> None:
    mcp.add_tool(
        kusto_service.kusto_known_services,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_command,
        annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
    )
    mcp.add_tool(
        kusto_service.kusto_list_entities,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_describe_database,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_describe_database_entity,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_graph_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_sample_entity,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_ingest_inline_into_table,
        annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_get_shots,
        description=ShotsTableRef.build_tool_description(
            CONFIG.shots_table,
            embedding_configured=bool(CONFIG.open_ai_embedding_endpoint),
            has_default_cluster=bool(CONFIG.default_service),
        ),
        annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_deeplink_from_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_show_queryplan,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    mcp.add_tool(
        kusto_service.kusto_diagnostics,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
