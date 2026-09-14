import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_bundle_has_isolated_simulation_targets():
    bundle = yaml.safe_load((ROOT / "codigo/databricks.yml").read_text(encoding="utf-8"))
    targets = bundle["targets"]
    assert set(targets) == {"development", "staging-simulation", "production-simulation"}
    assert targets["development"]["default"] is True
    for name in ("staging-simulation", "production-simulation"):
        assert "simulation" in name
        assert targets[name]["variables"]["pipeline_development"] is False
    schemas = {target["variables"]["uc_schema"] for target in targets.values()}
    assert len(schemas) == 3


def test_manifest_v2_references_existing_evidence():
    manifest = json.loads((ROOT / "portfolio.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 2
    assert manifest["classification"] == "applied-academic-project"
    assert any("sintéticos" in item for item in manifest["limitations"]["es"])
    for evidence in manifest["evidence"]:
        assert (ROOT / evidence["path"]).exists(), evidence["path"]

