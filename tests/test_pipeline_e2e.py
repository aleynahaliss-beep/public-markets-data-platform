"""End-to-end regression: every fault injected by the generator must be caught by the matching rule."""

from dataclasses import replace

import pytest

from pmdp.config import Config
from pmdp.pipeline import run


@pytest.fixture(scope="module")
def result(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("pmdp")
    cfg = replace(Config(), raw_dir=str(base / "raw"), out_dir=str(base / "out"))
    res = run(cfg, regen=True)
    res["out_dir"] = base / "out"
    return res


def failed(result):
    return {r["rule_id"]: r["failed_rows"] for r in result["results"]}


def test_injected_faults_are_all_detected(result):
    f = failed(result)
    expected = {
        "INS_003": 1, "INS_004": 1, "INS_005": 1, "INS_007": 1, "INS_008": 2,
        "CPT_002": 1, "CPT_003": 1,
        "PRC_001": 2, "PRC_003": 2, "PRC_004": 2, "PRC_005": 6, "PRC_006": 4,
        "TRD_002": 3, "TRD_003": 1, "TRD_004": 5, "TRD_005": 4, "TRD_007": 2, "TRD_009": 4,
        "COL_003": 1,
    }
    for rule_id, n in expected.items():
        assert f[rule_id] == n, f"{rule_id}: expected {n}, got {f[rule_id]}"
    # 3 trades point at unknown instruments + every trade on the 4 instruments quarantined upstream
    assert f["TRD_006"] > 3


def test_rules_that_should_be_clean_pass(result):
    f = failed(result)
    for rule_id in ("INS_001", "INS_002", "INS_006", "INS_009", "INS_010", "CPT_001", "CPT_004", "CPT_005",
                    "PRC_002", "FXR_001", "FXR_002", "TRD_001", "TRD_008", "TRD_010", "TRD_011",
                    "SET_001", "SET_002", "SET_003", "COL_001", "COL_002"):
        assert f[rule_id] == 0, rule_id


def test_clean_plus_quarantine_equals_input(result):
    for s in result["summary"]:
        assert s["rows_clean"] + s["rows_quarantined"] == s["rows_in"], s["dataset"]


def test_reconciliation_matches_injection_plan(result):
    counts = {r["break_category"]: r["count"] for r in result["frames"]["recon"].groupBy("break_category").count().collect()}
    assert counts["QUANTITY_BREAK"] == 3
    assert counts["AMOUNT_BREAK"] == 3
    assert counts["SETTLE_DATE_BREAK"] == 2
    assert counts["MISSING_IN_BACK_OFFICE"] == 3
    assert counts["MISSING_IN_FRONT_OFFICE"] == 2          # the two BO-only orphans
    assert counts["FO_REJECTED_BY_DQ"] > 0                 # rejected FO trades are traceable in the BO feed


def test_risk_outputs_are_sane(result):
    var = {r["scope"]: r for r in result["frames"]["var"].collect()}
    assert var["PORTFOLIO"]["var_usd"] > 0
    assert var["PORTFOLIO"]["es_usd"] >= var["PORTFOLIO"]["var_usd"]
    assert var["PORTFOLIO"]["n_scenarios"] == 250
    standalone = sum(r["var_usd"] for s, r in var.items() if s != "PORTFOLIO")
    assert var["PORTFOLIO"]["var_usd"] < standalone                    # diversification across asset classes
    margin = result["frames"]["margin"].collect()
    assert {r["margin_action"] for r in margin} >= {"MARGIN_CALL", "COLLATERAL_RETURN", "NO_ACTION", "NO_CSA"}
    for r in margin:
        if r["csa_flag"] == "N":
            assert r["margin_amount_usd"] == 0.0


def test_all_sql_reports_produce_output(result):
    assert set(result["sql"]) == {
        "01_dq_scorecard", "02_dq_by_dimension", "03_recon_breaks_by_counterparty", "04_exposure_concentration",
        "05_margin_summary_by_rating", "06_top_positions_by_asset_class", "07_rolling_volatility"}
    assert all(n > 0 for n in result["sql"].values())
    assert (result["out_dir"] / "sql" / "01_dq_scorecard.csv").exists()
