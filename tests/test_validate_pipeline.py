import json
import tempfile
import unittest
from pathlib import Path

from jobs.validate_pipeline import validate_pipeline_run


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


class ValidatePipelineTest(unittest.TestCase):
    def make_valid_run(self, base: Path) -> Path:
        raw_run_dir = base / "raw" / "store=winmart" / "date=2026-07-02" / "run_id=unit_pipeline_run"
        out_dir = base / "warehouse"
        raw_run_dir.mkdir(parents=True)

        bronze_dir = out_dir / "bronze" / "raw_records" / "store=winmart" / "date=2026-07-02" / "run_id=unit_pipeline_run"
        silver_dir = out_dir / "silver" / "store=winmart" / "date=2026-07-02" / "run_id=unit_pipeline_run"
        gold_dir = out_dir / "gold" / "fact_price_snapshot_daily" / "store=winmart" / "date=2026-07-02" / "run_id=unit_pipeline_run"

        write_jsonl(bronze_dir / "bronze_raw_records.jsonl", [{"bronze_record_key": "b1"}, {"bronze_record_key": "b2"}])
        write_jsonl(silver_dir / "retailer_products.jsonl", [{"retailer_product_id": "p1"}])
        write_jsonl(silver_dir / "product_observations.jsonl", [{"observation_id": "o1"}])
        write_jsonl(gold_dir / "price_snapshot_daily.jsonl", [{"price_snapshot_id": "g1"}])

        write_json(
            bronze_dir / "manifest.json",
            {
                "status": "success",
                "records_written": 2,
                "distinct_bronze_keys": 2,
                "duplicate_bronze_keys": 0,
                "invalid_json_records": 0,
                "quarantine_records": 0,
                "input_files": [{"source_dataset": "products_enriched", "records_written": 1}],
                "quality_status_counts": {"PASS": 2},
                "warning_counts": {},
            },
        )
        write_json(
            silver_dir / "manifest.json",
            {
                "status": "success",
                "product_dataset": "products_enriched",
                "retailer_products_written": 1,
                "product_observations_written": 1,
                "skipped_counts": {},
                "package_parse_status_counts": {"parsed_measurement": 1},
                "unit_price_publishable_counts": {"true": 1},
            },
        )
        write_json(
            gold_dir / "manifest.json",
            {
                "status": "success",
                "validation_status": "passed",
                "products_read": 1,
                "observations_read": 1,
                "snapshots_written": 1,
                "block_publish_issues": 0,
                "warn_issues": 0,
                "selection_stats": {"duplicate_observation_groups": 0, "observations_dropped": 0},
                "unit_price_publishable_counts": {"true": 1},
            },
        )
        write_json(
            gold_dir / "validation_report.json",
            {
                "status": "passed",
                "rows_checked": 1,
                "block_publish_issues": 0,
                "warn_issues": 0,
                "issue_counts": {},
            },
        )
        return raw_run_dir

    def test_validate_pipeline_passes_for_clean_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_run_dir = self.make_valid_run(base)

            report = validate_pipeline_run(raw_run_dir=raw_run_dir, out_dir=base / "warehouse")

            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["airflow_decision"]["task_status"], "success")
            self.assertTrue(report["airflow_decision"]["should_publish_gold"])
            self.assertFalse(report["airflow_decision"]["should_block_downstream"])
            self.assertFalse(report["airflow_decision"]["should_alert"])
            self.assertEqual(report["severity_counts"], {})
            self.assertEqual(report["layers"]["gold"]["snapshots_written"], 1)

    def test_validate_pipeline_fails_for_duplicate_bronze_key_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_run_dir = self.make_valid_run(base)
            bronze_manifest = (
                base
                / "warehouse"
                / "bronze"
                / "raw_records"
                / "store=winmart"
                / "date=2026-07-02"
                / "run_id=unit_pipeline_run"
                / "manifest.json"
            )
            manifest = json.loads(bronze_manifest.read_text(encoding="utf-8"))
            manifest["duplicate_bronze_keys"] = 1
            write_json(bronze_manifest, manifest)

            report = validate_pipeline_run(raw_run_dir=raw_run_dir, out_dir=base / "warehouse")

            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["airflow_decision"]["task_status"], "failed")
            self.assertFalse(report["airflow_decision"]["should_publish_gold"])
            self.assertTrue(report["airflow_decision"]["should_block_downstream"])
            self.assertTrue(report["airflow_decision"]["should_alert"])
            self.assertIn("duplicate_bronze_keys", report["airflow_decision"]["block_rules"])
            self.assertIn("BLOCK_RUN", report["severity_counts"])
            self.assertIn("duplicate_bronze_keys", {issue["rule_id"] for issue in report["issues"]})


if __name__ == "__main__":
    unittest.main()
