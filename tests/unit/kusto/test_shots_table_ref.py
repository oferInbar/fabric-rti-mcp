import pytest

from fabric_rti_mcp.services.kusto.shots_table_ref import ShotsTableRef

from unittest.mock import MagicMock, patch


class TestShotsTableRefParse:
    def test_plain_table_name(self) -> None:
        ref = ShotsTableRef.parse("MCP_SHOTS")
        assert ref.table_name == "MCP_SHOTS"
        assert ref.cluster_uri is None
        assert ref.database is None

    def test_fqn(self) -> None:
        ref = ShotsTableRef.parse(
            "cluster('asiusagetelemetryprod.eastus').database('InternalKnowledge').MCP_SHOTS"
        )
        assert ref.cluster_uri == "https://asiusagetelemetryprod.eastus.kusto.windows.net"
        assert ref.database == "InternalKnowledge"
        assert ref.table_name == "MCP_SHOTS"

    def test_fqn_with_https_uri(self) -> None:
        ref = ShotsTableRef.parse(
            "cluster('https://mycluster.eastus.kusto.windows.net').database('MyDB').Shots"
        )
        assert ref.cluster_uri == "https://mycluster.eastus.kusto.windows.net"
        assert ref.database == "MyDB"
        assert ref.table_name == "Shots"

    def test_fqn_case_insensitive(self) -> None:
        ref = ShotsTableRef.parse("Cluster('host').Database('db').Tbl")
        assert ref.cluster_uri == "https://host.kusto.windows.net"
        assert ref.database == "db"
        assert ref.table_name == "Tbl"

    def test_strips_whitespace(self) -> None:
        ref = ShotsTableRef.parse("  cluster('h').database('d').T  ")
        assert ref.cluster_uri == "https://h.kusto.windows.net"
        assert ref.database == "d"
        assert ref.table_name == "T"

    def test_plain_name_strips_whitespace(self) -> None:
        ref = ShotsTableRef.parse("  MyTable  ")
        assert ref.table_name == "MyTable"
        assert ref.cluster_uri is None

    def test_fqn_with_dots_in_cluster(self) -> None:
        ref = ShotsTableRef.parse(
            "cluster('my.cluster.region.kusto.windows.net').database('DB').Table"
        )
        assert ref.cluster_uri == "https://my.cluster.region.kusto.windows.net"
        assert ref.database == "DB"
        assert ref.table_name == "Table"

    def test_fqn_with_double_quotes(self) -> None:
        ref = ShotsTableRef.parse('cluster("myhost").database("mydb").MyTable')
        assert ref.cluster_uri == "https://myhost.kusto.windows.net"
        assert ref.database == "mydb"
        assert ref.table_name == "MyTable"

    def test_fqn_with_mixed_quotes_does_not_match(self) -> None:
        ref = ShotsTableRef.parse("""cluster("host').database('db").T""")
        assert ref.cluster_uri is None
        assert ref.table_name.startswith("cluster(")

    def test_short_cluster_name_auto_expanded(self) -> None:
        ref = ShotsTableRef.parse("cluster('asiusagetelemetryprod.eastus').database('DB').T")
        assert ref.cluster_uri == "https://asiusagetelemetryprod.eastus.kusto.windows.net"

    def test_full_uri_not_double_expanded(self) -> None:
        ref = ShotsTableRef.parse(
            "cluster('https://mycluster.eastus.kusto.windows.net').database('DB').T"
        )
        assert ref.cluster_uri == "https://mycluster.eastus.kusto.windows.net"

    def test_cluster_ending_with_suffix_gets_https_only(self) -> None:
        ref = ShotsTableRef.parse(
            "cluster('mycluster.eastus.kusto.windows.net').database('DB').T"
        )
        assert ref.cluster_uri == "https://mycluster.eastus.kusto.windows.net"

    def test_database_qualified(self) -> None:
        ref = ShotsTableRef.parse("database('InternalKnowledge').MCP_SHOTS")
        assert ref.database == "InternalKnowledge"
        assert ref.table_name == "MCP_SHOTS"
        assert ref.cluster_uri is None

    def test_database_qualified_double_quotes(self) -> None:
        ref = ShotsTableRef.parse('database("InternalKnowledge").MCP_SHOTS')
        assert ref.database == "InternalKnowledge"
        assert ref.table_name == "MCP_SHOTS"
        assert ref.cluster_uri is None

    def test_blank_value_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be blank"):
            ShotsTableRef.parse("   ")

    def test_fqn_with_blank_table_raises(self) -> None:
        with pytest.raises(ValueError, match="missing the table name"):
            ShotsTableRef.parse("cluster('h').database('d').   ")

    def test_database_qualified_with_blank_table_raises(self) -> None:
        with pytest.raises(ValueError, match="missing the table name"):
            ShotsTableRef.parse("database('d').   ")

    def test_http_cluster_uri_rejected(self) -> None:
        with pytest.raises(ValueError, match="HTTP cluster URIs are not allowed"):
            ShotsTableRef.parse("cluster('http://bad.kusto.windows.net').database('d').T")


class TestShotsTableRefIsFullyQualified:
    def test_fqn_is_fully_qualified(self) -> None:
        ref = ShotsTableRef.parse("cluster('h').database('d').T")
        assert ref.is_fully_qualified is True

    def test_plain_name_is_not_fully_qualified(self) -> None:
        ref = ShotsTableRef.parse("MyTable")
        assert ref.is_fully_qualified is False

    def test_manual_partial_ref(self) -> None:
        ref = ShotsTableRef(table_name="T", cluster_uri="h", database=None)
        assert ref.is_fully_qualified is False

    def test_database_only_is_not_fully_qualified(self) -> None:
        ref = ShotsTableRef.parse("database('db').T")
        assert ref.is_fully_qualified is False
        assert ref.database == "db"


class TestGetShotsResolution:
    """Test parameter resolution logic in kusto_get_shots."""

    @patch("fabric_rti_mcp.services.kusto.kusto_service._execute")
    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_fqn_config_needs_no_params(self, mock_config: MagicMock, mock_execute: MagicMock) -> None:
        """With a fully-qualified KUSTO_SHOTS_TABLE, the agent doesn't need to pass cluster or table."""
        ref = ShotsTableRef.parse("cluster('myhost').database('mydb').MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = "https://embed"
        mock_execute.return_value = {"data": {}, "format": "columnar"}

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        kusto_get_shots(prompt="show me errors")

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert call_args[0][1] == "https://myhost.kusto.windows.net"
        assert call_args[1]["database"] == "mydb"

    @patch("fabric_rti_mcp.services.kusto.kusto_service._execute")
    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_database_qualified_with_explicit_cluster(self, mock_config: MagicMock, mock_execute: MagicMock) -> None:
        """With database('db').Table config, agent provides cluster_uri only."""
        ref = ShotsTableRef.parse("database('mydb').MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = "https://embed"
        mock_execute.return_value = {"data": {}, "format": "columnar"}

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        kusto_get_shots(prompt="show me errors", cluster_uri="https://myhost")

        call_args = mock_execute.call_args
        assert call_args[0][1] == "https://myhost"
        assert call_args[1]["database"] == "mydb"
        assert "MyTable" in call_args[0][0]

    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_database_qualified_without_cluster_raises(self, mock_config: MagicMock) -> None:
        """With database('db').Table config and no cluster_uri, should raise."""
        ref = ShotsTableRef.parse("database('mydb').MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = None
        mock_config.default_service = None

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        with pytest.raises(ValueError, match="cluster_uri must be provided"):
            kusto_get_shots(prompt="test")

    @patch("fabric_rti_mcp.services.kusto.kusto_service._execute")
    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_plain_table_with_explicit_cluster(self, mock_config: MagicMock, mock_execute: MagicMock) -> None:
        """With a plain table name, the agent must provide cluster_uri explicitly."""
        ref = ShotsTableRef.parse("MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = "https://embed"
        mock_execute.return_value = {"data": {}, "format": "columnar"}

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        kusto_get_shots(prompt="show me errors", cluster_uri="https://explicit.cluster")

        call_args = mock_execute.call_args
        assert call_args[0][1] == "https://explicit.cluster"

    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_plain_table_without_cluster_raises(self, mock_config: MagicMock) -> None:
        """With a plain table name, no cluster_uri param, and no default service, should raise."""
        ref = ShotsTableRef.parse("MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = None
        mock_config.default_service = None

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        with pytest.raises(ValueError, match="cluster_uri must be provided"):
            kusto_get_shots(prompt="test")

    @patch("fabric_rti_mcp.services.kusto.kusto_service._execute")
    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_plain_table_falls_back_to_default_service(self, mock_config: MagicMock, mock_execute: MagicMock) -> None:
        """With a plain table name and CONFIG.default_service, cluster falls back to default service URI."""
        ref = ShotsTableRef.parse("MyTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = None
        mock_config.default_service.service_uri = "https://default.kusto.windows.net"
        mock_execute.return_value = {"data": {}, "format": "columnar"}

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        kusto_get_shots(prompt="test")

        call_args = mock_execute.call_args
        assert call_args[0][1] == "https://default.kusto.windows.net"

    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_no_config_no_params_raises_table_error(self, mock_config: MagicMock) -> None:
        """With no config and no params at all, should raise about missing table."""
        mock_config.shots_table = None
        mock_config.open_ai_embedding_endpoint = None

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        with pytest.raises(ValueError, match="shots_table_name must be provided"):
            kusto_get_shots(prompt="test")

    @patch("fabric_rti_mcp.services.kusto.kusto_service._execute")
    @patch("fabric_rti_mcp.services.kusto.kusto_service.CONFIG")
    def test_explicit_params_override_fqn(self, mock_config: MagicMock, mock_execute: MagicMock) -> None:
        """Explicit params should override the FQN config values."""
        ref = ShotsTableRef.parse("cluster('cfg-host').database('cfg-db').CfgTable")
        mock_config.shots_table = ref
        mock_config.open_ai_embedding_endpoint = "https://embed"
        mock_execute.return_value = {"data": {}, "format": "columnar"}

        from fabric_rti_mcp.services.kusto.kusto_service import kusto_get_shots

        kusto_get_shots(
            prompt="test",
            cluster_uri="https://override-host",
            shots_table_name="OverrideTable",
            database="override-db",
        )

        call_args = mock_execute.call_args
        assert call_args[0][1] == "https://override-host"
        assert call_args[1]["database"] == "override-db"
        assert "OverrideTable" in call_args[0][0]


class TestBuildToolDescription:
    """Test the dynamic tool description builder."""

    def test_fully_qualified_ref(self) -> None:
        ref = ShotsTableRef.parse("cluster('h').database('d').T")
        desc = ShotsTableRef.build_tool_description(ref, embedding_configured=True)
        assert "cluster_uri is pre-configured" in desc
        assert "shots_table_name is pre-configured" in desc
        assert "database is pre-configured" in desc
        assert "embedding_endpoint is pre-configured" in desc

    def test_database_qualified_ref(self) -> None:
        ref = ShotsTableRef.parse("database('d').T")
        desc = ShotsTableRef.build_tool_description(ref, embedding_configured=False)
        assert "shots_table_name is pre-configured" in desc
        assert "database is pre-configured" in desc
        assert "cluster_uri is REQUIRED" in desc

    def test_plain_table_ref(self) -> None:
        ref = ShotsTableRef.parse("T")
        desc = ShotsTableRef.build_tool_description(ref, embedding_configured=False)
        assert "shots_table_name is pre-configured" in desc
        assert "cluster_uri is REQUIRED" in desc
        assert "database is optional" in desc

    def test_no_config(self) -> None:
        desc = ShotsTableRef.build_tool_description(None, embedding_configured=False)
        assert "cluster_uri is REQUIRED" in desc
        assert "shots_table_name is REQUIRED" in desc

    def test_no_ref_with_default_cluster(self) -> None:
        desc = ShotsTableRef.build_tool_description(None, embedding_configured=False, has_default_cluster=True)
        assert "cluster_uri is pre-configured" in desc
        assert "shots_table_name is REQUIRED" in desc

    def test_plain_table_with_default_cluster(self) -> None:
        ref = ShotsTableRef.parse("T")
        desc = ShotsTableRef.build_tool_description(ref, embedding_configured=False, has_default_cluster=True)
        assert "cluster_uri is pre-configured" in desc
        assert "shots_table_name is pre-configured" in desc
