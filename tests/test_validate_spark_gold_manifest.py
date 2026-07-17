import json
import tempfile
import unittest
from pathlib import Path

from jobs.validate_spark_gold_manifest import validate_spark_gold_manifest


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")


class ValidateSparkGoldManifestTest(unittest.TestCase):
    def test_validate_manifest_passes_for_replacement_ready_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "manifest.json"
            write_json(
                manifest_path,
                {
                    "engine": "spark",
                    "status": "success",
                    "retailer_id": "winmart",
                    "snapshot_date": "2026-07-02",
                    "source_run_id": "unit_spark_run",
                    "snapshots_written": 537,
                    "comparison": {
                        "snapshot_id_sets_match": True,
                        "critical_fields_match": True,
                        "replacement_ready": True,
                        "missing_in_spark": 0,
                        "extra_in_spark": 0,
                        "critical_field_mismatches": 0,
                    },
                },
            )

            report = validate_spark_gold_manifest(manifest_path)

            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["severity_counts"], {})
            self.assertTrue(report["replacement_ready"])
            self.assertEqual(report["issues"], [])

    def test_validate_manifest_fails_when_replacement_ready_is_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "manifest.json"
            write_json(
                manifest_path,
                {
                    "engine": "spark",
                    "status": "success",
                    "retailer_id": "winmart",
                    "snapshot_date": "2026-07-02",
                    "source_run_id": "unit_spark_run",
                    "snapshots_written": 537,
                    "comparison": {
                        "snapshot_id_sets_match": True,
                        "critical_fields_match": False,
                        "replacement_ready": False,
                        "missing_in_spark": 0,
                        "extra_in_spark": 0,
                        "critical_field_mismatches": 8,
                    },
                },
            )

            report = validate_spark_gold_manifest(manifest_path)

            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["replacement_ready"])
            self.assertIn("BLOCK_PUBLISH", report["severity_counts"])
            self.assertIn("replacement_not_ready", {issue["rule_id"] for issue in report["issues"]})
            self.assertIn("critical_fields_not_match", {issue["rule_id"] for issue in report["issues"]})


if __name__ == "__main__":
    unittest.main()
