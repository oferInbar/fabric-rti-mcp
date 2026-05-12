import os
import signal
import sys
import types
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from fabric_rti_mcp import __version__
from fabric_rti_mcp.authentication.auth_middleware import add_auth_middleware
from fabric_rti_mcp.compat.ms_foundry import SchemaCompatibleMCP
from fabric_rti_mcp.config import AH_MODE_VIBE_HUNTING, logger
from fabric_rti_mcp.config import global_config as config
from fabric_rti_mcp.config.obo import obo_config
from fabric_rti_mcp.services.hunting import hunting_tools

# Global variable to store server start time
server_start_time = datetime.now(timezone.utc)


def setup_shutdown_handler(sig: int, frame: types.FrameType | None) -> None:
    """Handle process termination signals."""
    signal_name = signal.Signals(sig).name
    logger.info(f"Received signal {sig} ({signal_name}), shutting down...")

    # Exit the process
    sys.exit(0)


AH_MODE_INSTRUCTIONS = (
    "This server provides access to Microsoft 365 Defender Advanced Hunting. "
    "Use the run_hunting_query tool to execute KQL queries against Defender tables such as "
    "DeviceProcessEvents, DeviceNetworkEvents, DeviceFileEvents, DeviceRegistryEvents, "
    "DeviceLogonEvents, DeviceImageLoadEvents, DeviceEvents, EmailEvents, EmailAttachmentInfo, "
    "EmailUrlInfo, IdentityLogonEvents, IdentityQueryEvents, IdentityDirectoryEvents, "
    "CloudAppEvents, AlertInfo, AlertEvidence, and more. "
    "Use the get_hunting_schema tool to discover all available tables and their columns. "
    "Queries use Kusto Query Language (KQL) syntax."
)

VIBE_HUNTING_INSTRUCTIONS = (
    AH_MODE_INSTRUCTIONS + " "
    "Additional tools are available for enrichment: use get_device_insights and get_user_insights "
    "to enrich entities found in results, analyze_hunting_results for visualization metadata, "
    "suggest_hunting_followups for pivot suggestions, get_available_hunting_actions for response actions, "
    "and summarize_hunting_results for natural language summaries of findings."
)


def register_tools(mcp: FastMCP) -> None:
    """Register hunting tools with the MCP server.

    Core Advanced Hunting tools are always registered. Enrichment tools are
    additionally registered when AH_MODE=VibeHunting (or a legacy truthy value).
    """
    hunting_tools.register_core_tools(mcp)
    if config.ah_mode == AH_MODE_VIBE_HUNTING:
        logger.info("AH_MODE=VibeHunting — registering enrichment tools")
        hunting_tools.register_enrichment_tools(mcp)
    else:
        logger.info("Registering core Advanced Hunting tools only (set AH_MODE=VibeHunting to enable enrichment)")


# Health check function defined at module level
async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint."""
    current_time = datetime.now(timezone.utc)
    logger.info(f"Server health check at {current_time}")
    return JSONResponse(
        {
            "status": "healthy",
            "current_time_utc": current_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "server": "fabric-rti-mcp",
            "start_time_utc": server_start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
    )


def add_health_endpoint(mcp: FastMCP) -> None:
    """Add health endpoint for Kubernetes liveness probes."""
    # Register the pre-defined health check function
    mcp.custom_route("/health", methods=["GET"])(health_check)


def main() -> None:
    """Main entry point for the server."""
    try:
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, setup_shutdown_handler)  # Signal Interrupt)
        signal.signal(signal.SIGTERM, setup_shutdown_handler)  # Signal Terminate

        # Set up logging to stderr because stdout is used for stdio transport
        # writing to stderr because stdout is used for the transport
        # and we want to see the logs in the console
        logger.info("Starting Fabric RTI MCP server")
        logger.info(f"Version: {__version__}")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Platform: {sys.platform}")
        logger.info(f"PID: {os.getpid()}")
        logger.info(f"Transport: {config.transport}")

        if config.transport == "http":
            logger.info(f"Host: {config.http_host}")
            logger.info(f"Port: {config.http_port}")
            logger.info(f"Path: {config.http_path}")
            logger.info(f"Stateless HTTP: {config.stateless_http}")
            logger.info(f"Use OBO flow: {config.use_obo_flow}")

        # TODO: Add telemetry configuration here

        if config.use_obo_flow and (not obo_config.entra_app_client_id or not obo_config.umi_client_id):
            raise ValueError("OBO flow is enabled but required client IDs are missing")

        if config.use_ai_foundry_compat:
            logger.info("AI Foundry compatibility mode enabled - schemas will be simplified")

        name = "fabric-rti-mcp-server"
        fastmcp_class = SchemaCompatibleMCP if config.use_ai_foundry_compat else FastMCP

        instructions = config.instructions
        if not instructions:
            instructions = VIBE_HUNTING_INSTRUCTIONS if config.ah_mode == AH_MODE_VIBE_HUNTING else AH_MODE_INSTRUCTIONS

        if config.transport == "http":
            fastmcp_server = fastmcp_class(
                name,
                instructions=instructions,
                host=config.http_host,
                port=config.http_port,
                streamable_http_path=config.http_path,
                stateless_http=config.stateless_http,
            )
        else:
            fastmcp_server = fastmcp_class(name, instructions=instructions)

        # 1. Register tools
        register_tools(fastmcp_server)

        # 2. Add HTTP-specific features if in HTTP mode
        if config.transport == "http":
            add_health_endpoint(fastmcp_server)
            if os.getenv("FABRIC_RTI_DISABLE_AUTH", "").lower() in ("true", "1"):
                logger.warning("Authorization middleware DISABLED (FABRIC_RTI_DISABLE_AUTH is set)")
            else:
                logger.info("Adding authorization middleware")
                add_auth_middleware(fastmcp_server)

        # TBD - Add telemetry

        # 3. Run the server with the specified transport
        if config.transport == "http":
            logger.info(f"Starting {name} (HTTP) on {config.http_host}:{config.http_port} with /health endpoint")
            fastmcp_server.run(transport="streamable-http")
        else:
            logger.info(f"Starting {name} (stdio)")
            fastmcp_server.run(transport="stdio")

    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as error:
        logger.error(f"Server error: {error}")
        raise


if __name__ == "__main__":
    main()
