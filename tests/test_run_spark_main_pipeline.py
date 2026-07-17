import json
import tempfile
import unittest
from pathlib import Path

from jobs.run_spark_main_pipeline import find_latest_raw_run, should_skip_run


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")


class RunSparkMainPipelineHelpersTest(unittest.TestCase):
    def test_find_latest_raw_run_uses_latest_date_and_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_store_dir = Path(tmp) / "raw" / "store=winmart"
            (raw_store_dir / "date=2026-07-02" / "run_id=20260702_100000").mkdir(parents=True)
            (raw_store_dir / "date=2026-07-02" / "run_id=20260702_120000").mkdir(parents=True)
            (raw_store_dir / "date=2026-07-03" / "run_id=20260703_090000").mkdir(parents=True)

            latest = find_latest_raw_run(raw_store_dir)

            self.assertEqual(latest.as_posix(), (raw_store_dir / "date=2026-07-03" / "run_id=20260703_090000").as_posix())

    def test_should_skip_run_only_when_same_successful_run_manifest_exists_and_same_output_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_run_dir = base / "raw" / "store=winmart" / "date=2026-07-03" / "run_id=20260703_090000"
            raw_run_dir.mkdir(parents=True)
            manifest_path = base / "warehouse_spark" / "gold" / "fact_price_snapshot_daily" / "store=winmart" / "date=2026-07-03" / "run_id=20260703_090000" / "manifest.json"
            write_json(manifest_path, {"status": "success"})

            state = {
                "last_processed_raw_run": raw_run_dir.as_posix(),
                "last_status": "success",
                "spark_output_format": "parquet",
            }

            self.assertTrue(should_skip_run(state, raw_run_dir, manifest_path, force=False, spark_output_format="parquet"))
            self.assertFalse(should_skip_run(state, raw_run_dir, manifest_path, force=True, spark_output_format="parquet"))
            self.assertFalse(should_skip_run(state, raw_run_dir, manifest_path, force=False, spark_output_format="hudi"))
            self.assertFalse(
                should_skip_run(
                    {"last_processed_raw_run": raw_run_dir.as_posix(), "last_status": "failed", "spark_output_format": "parquet"},
                    raw_run_dir,
                    manifest_path,
                    force=False,
                    spark_output_format="parquet",
                )
            )


if __name__ == "__main__":
    unittest.main()
