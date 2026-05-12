from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass

logger = logging.getLogger("fabric-rti-mcp")


class GlobalFabricRTIEnvVarNames:
    http_host = "FABRIC_RTI_HTTP_HOST"
    transport = "FABRIC_RTI_TRANSPORT"
    http_port = "FABRIC_RTI_HTTP_PORT"  # default port name used by RTI MCP
    azure_service_deployment_default_port = "PORT"  # Azure App Services or Azure Container Apps uses this port name
    functions_deployment_default_port = "FUNCTIONS_CUSTOMHANDLER_PORT"  # Azure Functions uses this port name
    http_path = "FABRIC_RTI_HTTP_PATH"
    stateless_http = "FABRIC_RTI_STATELESS_HTTP"
    use_obo_flow = "USE_OBO_FLOW"
    use_ai_foundry_compat = "FABRIC_RTI_AI_FOUNDRY_COMPATIBILITY_SCHEMA"
    cors_allowed_origins = "FABRIC_RTI_CORS_ORIGINS"
    ah_mode = "AH_MODE"
    instructions = "FABRIC_RTI_INSTRUCTIONS"


DEFAULT_FABRIC_RTI_TRANSPORT = "stdio"
DEFAULT_FABRIC_RTI_HTTP_PORT = 3000
DEFAULT_FABRIC_RTI_HTTP_PATH = "/mcp"
DEFAULT_FABRIC_RTI_HTTP_HOST = "127.0.0.1"
DEFAULT_FABRIC_RTI_STATELESS_HTTP = False
DEFAULT_USE_OBO_FLOW = False
DEFAULT_FABRIC_RTI_AI_FOUNDRY_COMPATIBILITY_SCHEMA = False
DEFAULT_FABRIC_RTI_CORS_ORIGINS = "*"


AH_MODE_ADVANCED_HUNTING = "AdvancedHunting"
AH_MODE_VIBE_HUNTING = "VibeHunting"
_VALID_AH_MODES = {"", AH_MODE_ADVANCED_HUNTING, AH_MODE_VIBE_HUNTING}
# Legacy boolean values map to VibeHunting for backward compatibility
_LEGACY_TRUE_VALUES = {"true", "1"}


@dataclass(slots=True, frozen=True)
class GlobalFabricRTIConfig:
    transport: str
    http_host: str
    http_port: int
    http_path: str
    stateless_http: bool
    use_obo_flow: bool
    use_ai_foundry_compat: bool
    cors_allowed_origins: str
    ah_mode: str
    instructions: str | None

    @staticmethod
    def _parse_ah_mode(value: str) -> str:
        """Parse AH_MODE value, supporting legacy bool and new enum values."""
        stripped = value.strip()
        if stripped.lower() in _LEGACY_TRUE_VALUES:
            return AH_MODE_VIBE_HUNTING
        if stripped in _VALID_AH_MODES:
            return stripped
        # Case-insensitive match
        for valid in _VALID_AH_MODES:
            if stripped.lower() == valid.lower():
                return valid
        logger.warning(f"Unknown AH_MODE value '{stripped}', defaulting to empty (disabled)")
        return ""

    @staticmethod
    def from_env() -> GlobalFabricRTIConfig:
        return GlobalFabricRTIConfig(
            transport=os.getenv(GlobalFabricRTIEnvVarNames.transport, DEFAULT_FABRIC_RTI_TRANSPORT),
            http_host=os.getenv(GlobalFabricRTIEnvVarNames.http_host, DEFAULT_FABRIC_RTI_HTTP_HOST),
            http_port=int(
                os.getenv(
                    "PORT",
                    os.getenv(
                        "FUNCTIONS_CUSTOMHANDLER_PORT",
                        os.getenv(GlobalFabricRTIEnvVarNames.http_port, DEFAULT_FABRIC_RTI_HTTP_PORT),
                    ),
                )
            ),
            http_path=os.getenv(GlobalFabricRTIEnvVarNames.http_path, DEFAULT_FABRIC_RTI_HTTP_PATH),
            stateless_http=os.getenv(GlobalFabricRTIEnvVarNames.stateless_http, "false").lower() in ("true", "1"),
            use_obo_flow=os.getenv(GlobalFabricRTIEnvVarNames.use_obo_flow, "false").lower() in ("true", "1"),
            use_ai_foundry_compat=os.getenv(GlobalFabricRTIEnvVarNames.use_ai_foundry_compat, "false").lower()
            in ("true", "1"),
            cors_allowed_origins=os.getenv(
                GlobalFabricRTIEnvVarNames.cors_allowed_origins, DEFAULT_FABRIC_RTI_CORS_ORIGINS
            ),
            ah_mode=GlobalFabricRTIConfig._parse_ah_mode(os.getenv(GlobalFabricRTIEnvVarNames.ah_mode, "")),
            instructions=os.getenv(GlobalFabricRTIEnvVarNames.instructions, None),
        )

    @staticmethod
    def existing_env_vars() -> list[str]:
        """Return a list of environment variable names that are currently set."""
        result: list[str] = []
        env_vars = [
            GlobalFabricRTIEnvVarNames.transport,
            GlobalFabricRTIEnvVarNames.http_host,
            GlobalFabricRTIEnvVarNames.http_port,
            GlobalFabricRTIEnvVarNames.http_path,
            GlobalFabricRTIEnvVarNames.stateless_http,
            GlobalFabricRTIEnvVarNames.use_obo_flow,
            GlobalFabricRTIEnvVarNames.use_ai_foundry_compat,
            GlobalFabricRTIEnvVarNames.cors_allowed_origins,
            GlobalFabricRTIEnvVarNames.ah_mode,
            GlobalFabricRTIEnvVarNames.instructions,
        ]
        for env_var in env_vars:
            if os.getenv(env_var) is not None:
                result.append(env_var)
        return result

    @staticmethod
    def with_args() -> GlobalFabricRTIConfig:
        base_config = GlobalFabricRTIConfig.from_env()

        # see if the client is passing these (ex: local debug / test client)
        parser = argparse.ArgumentParser(description="Fabric RTI MCP Server")
        parser.add_argument("--stdio", action="store_true", help="Use stdio transport")
        parser.add_argument("--http", action="store_true", help="Use HTTP transport")
        parser.add_argument("--host", type=str, help="HTTP host to listen on")
        parser.add_argument("--port", type=int, help="HTTP port to listen on")
        parser.add_argument("--stateless-http", action="store_true", help="Enable or disable stateless HTTP")
        parser.add_argument("--use-obo-flow", action="store_true", help="Enable or disable OBO flow")
        parser.add_argument(
            "--use-ai-foundry-compat", action="store_true", help="Enable or disable AI Foundry compatibility mode"
        )
        args, _ = parser.parse_known_args()

        transport = base_config.transport
        if args.stdio:
            transport = "stdio"
        elif args.http or os.getenv("PORT"):  # if it is running in Azure (Port is set), use HTTP transport
            transport = "http"

        stateless_http = args.stateless_http if "--stateless-http" in sys.argv else base_config.stateless_http
        http_host = args.host if args.host is not None else base_config.http_host
        http_port = args.port if args.port is not None else base_config.http_port
        use_obo_flow = args.use_obo_flow if "--use-obo-flow" in sys.argv else base_config.use_obo_flow
        use_ai_foundry_compat = (
            args.use_ai_foundry_compat if "--use-ai-foundry-compat" in sys.argv else base_config.use_ai_foundry_compat
        )

        return GlobalFabricRTIConfig(
            transport=transport,
            http_host=http_host,
            http_port=http_port,
            http_path=base_config.http_path,
            stateless_http=stateless_http,
            use_obo_flow=use_obo_flow,
            use_ai_foundry_compat=use_ai_foundry_compat,
            cors_allowed_origins=base_config.cors_allowed_origins,
            ah_mode=base_config.ah_mode,
            instructions=base_config.instructions,
        )


# Global configuration instance
global_config = GlobalFabricRTIConfig.with_args()
