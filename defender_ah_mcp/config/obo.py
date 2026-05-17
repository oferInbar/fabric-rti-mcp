import argparse
import os
from dataclasses import dataclass

from defender_ah_mcp.config import logger


class DefenderAdvancedHuntingMCPOBOFlowEnvVarNames:
    """Environment variable names for OBO Flow configuration."""

    azure_tenant_id = "DEFENDER_AH_AZURE_TENANT_ID"
    # client id for the AAD App which is used to authenticate the user from gateway (APIM)
    entra_app_client_id = "DEFENDER_AH_ENTRA_APP_CLIENT_ID"
    # user assigned managed identity client id used as Federated credentials on the Entra App (entra_app_client_id)
    umi_client_id = "DEFENDER_AH_USER_MANAGED_IDENTITY_CLIENT_ID"
    # Audience the OBO-exchanged token targets.
    token_audience = "DEFENDER_AH_TOKEN_AUDIENCE"


DEFAULT_DEFENDER_AH_ENTRA_APP_CLIENT_ID = ""
DEFAULT_DEFENDER_AH_USER_MANAGED_IDENTITY_CLIENT_ID = ""
DEFAULT_DEFENDER_AH_TOKEN_AUDIENCE = "https://graph.microsoft.com/.default"


@dataclass(slots=True, frozen=True)
class DefenderAdvancedHuntingMCPOBOFlowAuthConfig:
    """Configuration for OBO (On-Behalf-Of) Flow authentication."""

    azure_tenant_id: str
    entra_app_client_id: str
    umi_client_id: str
    token_audience: str

    @staticmethod
    def from_env() -> "DefenderAdvancedHuntingMCPOBOFlowAuthConfig":
        """Load OBO Flow configuration from environment variables.

        The tenant ID has no default — it must be supplied by the deployment
        environment when OBO flow is enabled.
        """
        tenant_id = os.getenv(DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.azure_tenant_id, "")
        if not tenant_id:
            logger.debug(
                f"{DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.azure_tenant_id} is not set; "
                "OBO flow will fail until it is provided."
            )
        return DefenderAdvancedHuntingMCPOBOFlowAuthConfig(
            azure_tenant_id=tenant_id,
            entra_app_client_id=os.getenv(
                DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.entra_app_client_id,
                DEFAULT_DEFENDER_AH_ENTRA_APP_CLIENT_ID,
            ),
            umi_client_id=os.getenv(
                DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.umi_client_id,
                DEFAULT_DEFENDER_AH_USER_MANAGED_IDENTITY_CLIENT_ID,
            ),
            token_audience=os.getenv(
                DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.token_audience,
                DEFAULT_DEFENDER_AH_TOKEN_AUDIENCE,
            ),
        )

    @staticmethod
    def existing_env_vars() -> list[str]:
        """Return a list of environment variable names that are currently set."""
        env_vars = [
            DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.azure_tenant_id,
            DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.entra_app_client_id,
            DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.umi_client_id,
            DefenderAdvancedHuntingMCPOBOFlowEnvVarNames.token_audience,
        ]
        return [name for name in env_vars if os.getenv(name) is not None]

    @staticmethod
    def with_args() -> "DefenderAdvancedHuntingMCPOBOFlowAuthConfig":
        """Load OBO Flow configuration from environment variables and command line arguments."""
        obo_config = DefenderAdvancedHuntingMCPOBOFlowAuthConfig.from_env()

        parser = argparse.ArgumentParser(description="Defender Advanced Hunting MCP Server OBO Flow Configuration")
        parser.add_argument("--entra-app-client-id", type=str, help="Azure AAD App Client ID")
        parser.add_argument("--umi-client-id", type=str, help="User Managed Identity Client ID")
        args, _ = parser.parse_known_args()

        entra_app_client_id = (
            args.entra_app_client_id if args.entra_app_client_id is not None else obo_config.entra_app_client_id
        )
        umi_client_id = args.umi_client_id if args.umi_client_id is not None else obo_config.umi_client_id

        return DefenderAdvancedHuntingMCPOBOFlowAuthConfig(
            azure_tenant_id=obo_config.azure_tenant_id,
            entra_app_client_id=entra_app_client_id,
            umi_client_id=umi_client_id,
            token_audience=obo_config.token_audience,
        )


obo_config = DefenderAdvancedHuntingMCPOBOFlowAuthConfig.with_args()
