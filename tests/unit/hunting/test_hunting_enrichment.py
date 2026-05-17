from defender_ah_mcp.services.hunting.hunting_enrichment import (
    analyze_hunting_results,
    get_available_hunting_actions,
    suggest_hunting_followups,
    summarize_hunting_results,
)

# --- analyze_hunting_results ---


class TestAnalyzeHuntingResults:
    def test_analyze_identifies_timestamp_column(self):
        schema = [{"Name": "Timestamp", "Type": "DateTime"}, {"Name": "DeviceName", "Type": "String"}]
        results = [{"Timestamp": "2024-01-01T00:00:00Z", "DeviceName": "PC1"}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] == "Timestamp"

    def test_analyze_fallback_datetime_column(self):
        schema = [{"Name": "EventTime", "Type": "DateTime"}, {"Name": "Count", "Type": "Int64"}]
        results = [{"EventTime": "2024-01-01T00:00:00Z", "Count": 5}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] == "EventTime"

    def test_analyze_no_datetime_column(self):
        schema = [{"Name": "DeviceName", "Type": "String"}, {"Name": "Count", "Type": "Int64"}]
        results = [{"DeviceName": "PC1", "Count": 5}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] is None

    def test_analyze_chart_type_line_for_timeseries(self):
        schema = [{"Name": "Timestamp", "Type": "DateTime"}, {"Name": "Count", "Type": "Int64"}]
        results = [{"Timestamp": "2024-01-01", "Count": 5}, {"Timestamp": "2024-01-02", "Count": 10}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["chart_type"] == "line"

    def test_analyze_chart_type_card_for_single_row(self):
        schema = [{"Name": "Count", "Type": "Int64"}]
        results = [{"Count": 42}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["chart_type"] == "card"

    def test_analyze_chart_type_bar_for_categorical(self):
        schema = [{"Name": "Severity", "Type": "String"}, {"Name": "Count", "Type": "Int64"}]
        results = [
            {"Severity": "High", "Count": 10},
            {"Severity": "Medium", "Count": 20},
            {"Severity": "Low", "Count": 5},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["chart_type"] == "bar"

    def test_analyze_filter_suggestions(self):
        schema = [{"Name": "Severity", "Type": "String"}, {"Name": "DeviceName", "Type": "String"}]
        results = [
            {"Severity": "High", "DeviceName": "PC1"},
            {"Severity": "Medium", "DeviceName": "PC1"},
            {"Severity": "Low", "DeviceName": "PC2"},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert len(analysis["filter_suggestions"]) >= 1
        filter_names = [f["column"] for f in analysis["filter_suggestions"]]
        assert "Severity" in filter_names

    def test_analyze_empty_results(self):
        schema = [{"Name": "Timestamp", "Type": "DateTime"}]
        results = []
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] == "Timestamp"
        assert analysis["chart_type"] == "table"
        assert analysis["filter_suggestions"] == []

    def test_analyze_column_stats(self):
        schema = [{"Name": "Timestamp", "Type": "DateTime"}, {"Name": "Count", "Type": "Int64"}]
        results = [
            {"Timestamp": "2024-01-01", "Count": 5},
            {"Timestamp": "2024-01-02", "Count": 10},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert "Timestamp" in analysis["column_stats"]
        assert analysis["column_stats"]["Timestamp"]["distinct_count"] == 2
        assert "min" in analysis["column_stats"]["Timestamp"]
        assert "max" in analysis["column_stats"]["Timestamp"]
        assert analysis["column_stats"]["Count"]["min"] == 5.0
        assert analysis["column_stats"]["Count"]["max"] == 10.0


# --- suggest_hunting_followups ---


class TestSuggestHuntingFollowups:
    def test_suggest_followups_table_pivot(self):
        query = "DeviceProcessEvents | where FileName == 'cmd.exe' | limit 10"
        schema = [{"Name": "DeviceId", "Type": "String"}, {"Name": "FileName", "Type": "String"}]
        suggestions = suggest_hunting_followups(query, schema)
        assert len(suggestions) > 0
        descriptions = [s["description"] for s in suggestions]
        assert any("network" in d.lower() for d in descriptions)

    def test_suggest_followups_column_pivot_sha(self):
        query = "DeviceFileEvents | limit 10"
        schema = [{"Name": "SHA256", "Type": "String"}, {"Name": "FileName", "Type": "String"}]
        suggestions = suggest_hunting_followups(query, schema)
        descriptions = [s["description"] for s in suggestions]
        assert any("file reputation" in d.lower() for d in descriptions)

    def test_suggest_followups_column_pivot_ip(self):
        query = "DeviceNetworkEvents | limit 10"
        schema = [{"Name": "RemoteIP", "Type": "String"}, {"Name": "DeviceId", "Type": "String"}]
        suggestions = suggest_hunting_followups(query, schema)
        descriptions = [s["description"] for s in suggestions]
        assert any("ip" in d.lower() or "sweep" in d.lower() for d in descriptions)

    def test_suggest_followups_max_five(self):
        query = "AlertInfo | limit 10"
        schema = [
            {"Name": "SHA256", "Type": "String"},
            {"Name": "RemoteIP", "Type": "String"},
            {"Name": "AccountUpn", "Type": "String"},
            {"Name": "DeviceId", "Type": "String"},
            {"Name": "AlertId", "Type": "String"},
            {"Name": "NetworkMessageId", "Type": "String"},
        ]
        suggestions = suggest_hunting_followups(query, schema)
        assert len(suggestions) <= 5

    def test_suggest_followups_no_match(self):
        query = "CustomTable | limit 10"
        schema = [{"Name": "CustomColumn", "Type": "String"}]
        suggestions = suggest_hunting_followups(query, schema)
        assert len(suggestions) == 0

    def test_suggest_followups_deduplication(self):
        query = "DeviceFileEvents | limit 10"
        schema = [{"Name": "DeviceId", "Type": "String"}, {"Name": "SHA256", "Type": "String"}]
        suggestions = suggest_hunting_followups(query, schema)
        descriptions = [s["description"] for s in suggestions]
        assert len(descriptions) == len(set(descriptions))


# --- get_available_hunting_actions ---


class TestGetAvailableHuntingActions:
    def test_actions_device_columns(self):
        actions = get_available_hunting_actions(["DeviceId", "DeviceName", "Timestamp"])
        action_names = [a["action"] for a in actions]
        assert "IsolateMachine" in action_names
        assert "CollectInvestigationPackage" in action_names
        assert "RunAntivirusScan" in action_names

    def test_actions_file_columns(self):
        actions = get_available_hunting_actions(["SHA1", "FileName"])
        action_names = [a["action"] for a in actions]
        assert "StopAndQuarantineFile" in action_names
        assert "BlockFile" in action_names

    def test_actions_user_columns(self):
        actions = get_available_hunting_actions(["AccountUpn", "AccountName"])
        action_names = [a["action"] for a in actions]
        assert "DisableUser" in action_names
        assert "ForceUserPasswordReset" in action_names

    def test_actions_email_columns(self):
        actions = get_available_hunting_actions(["NetworkMessageId", "Subject"])
        action_names = [a["action"] for a in actions]
        assert "DeleteEmail" in action_names
        assert "MoveEmailToFolder" in action_names

    def test_actions_case_insensitive(self):
        actions = get_available_hunting_actions(["deviceid", "sha256"])
        action_names = [a["action"] for a in actions]
        assert "IsolateMachine" in action_names
        assert "StopAndQuarantineFile" in action_names

    def test_actions_no_matching_columns(self):
        actions = get_available_hunting_actions(["Timestamp", "CustomColumn"])
        assert len(actions) == 0

    def test_actions_mixed_columns(self):
        actions = get_available_hunting_actions(["DeviceId", "SHA256", "AccountUpn", "NetworkMessageId"])
        entity_types = {a["entity_type"] for a in actions}
        assert entity_types == {"Device", "File", "User", "Email"}


# --- Enhanced analyze_hunting_results (new features) ---


class TestAnalyzeHuntingResultsEnhanced:
    def test_timeline_column_priority_ranking(self):
        """Timestamp should be preferred over other datetime columns."""
        schema = [
            {"Name": "CreatedDateTime", "Type": "DateTime"},
            {"Name": "Timestamp", "Type": "DateTime"},
            {"Name": "LastSeen", "Type": "DateTime"},
        ]
        results = [{"CreatedDateTime": "2024-01-01", "Timestamp": "2024-01-02", "LastSeen": "2024-01-03"}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] == "Timestamp"

    def test_timeline_column_eventtime_over_lastseen(self):
        """EventTime should rank higher than LastSeen."""
        schema = [
            {"Name": "LastSeen", "Type": "DateTime"},
            {"Name": "EventTime", "Type": "DateTime"},
        ]
        results = [{"LastSeen": "2024-01-01", "EventTime": "2024-01-02"}]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["timeline_column"] == "EventTime"

    def test_chart_type_bar_for_entity_plus_aggregate(self):
        """Entity column + numeric aggregate → bar chart even with high cardinality."""
        schema = [{"Name": "AccountUpn", "Type": "String"}, {"Name": "FailedAttempts", "Type": "Int64"}]
        results = [{"AccountUpn": f"user{i}@contoso.com", "FailedAttempts": i} for i in range(50)]
        analysis = analyze_hunting_results(schema, results)
        assert analysis["chart_type"] == "bar"

    def test_summary_included(self):
        """Summary stats should be present in the output."""
        schema = [{"Name": "DeviceName", "Type": "String"}, {"Name": "Count", "Type": "Int64"}]
        results = [
            {"DeviceName": "PC1", "Count": 10},
            {"DeviceName": "PC1", "Count": 20},
            {"DeviceName": "PC2", "Count": 5},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert "summary" in analysis
        assert analysis["summary"]["total_rows"] == 3

    def test_summary_top_entities(self):
        """Summary should identify top entities."""
        schema = [{"Name": "DeviceName", "Type": "String"}, {"Name": "Severity", "Type": "String"}]
        results = [
            {"DeviceName": "PC1", "Severity": "High"},
            {"DeviceName": "PC1", "Severity": "Medium"},
            {"DeviceName": "PC2", "Severity": "Low"},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert "top_entities" in analysis["summary"]
        assert "DeviceName" in analysis["summary"]["top_entities"]
        assert analysis["summary"]["top_entities"]["DeviceName"][0] == "PC1"

    def test_summary_anomaly_hint_concentration(self):
        """Should flag when >80% of rows share a single value."""
        schema = [{"Name": "RemoteIP", "Type": "String"}]
        results = [{"RemoteIP": "10.0.0.1"}] * 9 + [{"RemoteIP": "10.0.0.2"}]
        analysis = analyze_hunting_results(schema, results)
        assert "anomaly_hints" in analysis["summary"]
        assert any("10.0.0.1" in hint for hint in analysis["summary"]["anomaly_hints"])

    def test_summary_time_range(self):
        """Summary should include time range when datetime column exists."""
        schema = [{"Name": "Timestamp", "Type": "DateTime"}, {"Name": "Value", "Type": "Int64"}]
        results = [
            {"Timestamp": "2024-01-01T00:00:00Z", "Value": 1},
            {"Timestamp": "2024-01-05T00:00:00Z", "Value": 2},
        ]
        analysis = analyze_hunting_results(schema, results)
        assert "time_range" in analysis["summary"]
        assert analysis["summary"]["time_range"]["min"] == "2024-01-01T00:00:00Z"
        assert analysis["summary"]["time_range"]["max"] == "2024-01-05T00:00:00Z"


# --- summarize_hunting_results ---


class TestSummarizeHuntingResults:
    def test_returns_unavailable_without_context(self):
        """Should gracefully handle missing context."""
        import asyncio

        result = asyncio.run(
            summarize_hunting_results(
                query="DeviceProcessEvents | limit 10",
                schema=[{"Name": "Timestamp", "Type": "DateTime"}],
                results=[{"Timestamp": "2024-01-01"}],
                ctx=None,
            )
        )
        assert result["source"] == "unavailable"
        assert result["summary_bullets"] == []
