import json
import tempfile
import unittest
from pathlib import Path

from jobs.bronze.ingest_raw import ingest_run


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class BronzeIngestTest(unittest.TestCase):
    def make_run_dir(self, base: Path) -> Path:
        run_dir = base / "raw" / "store=winmart" / "date=2026-06-29" / "run_id=unit_test_run"
        run_dir.mkdir(parents=True)
        metadata = {
            "run_id": "unit_test_run",
            "store_id": "winmart",
            "store_name": "WinMart",
            "config_api_summary": {
                "region": "Binh Dinh",
                "store_code": "1682",
                "store_group_code": "1998",
            },
        }
        (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
        write_jsonl(
            run_dir / "products.jsonl",
            [
                {
                    "crawl_timestamp": "2026-06-29T13:50:04+07:00",
                    "current_price": 135000,
                    "package_size_raw": None,
                    "product_name_raw": "Product A",
                    "source_product_id": "product-a",
                    "source_url": "https://api.example.test/products?a=1",
                    "store_code": "1682",
                },
                {
                    "crawl_timestamp": "2026-06-29T13:50:05+07:00",
                    "current_price": 10000,
                    "product_name_raw": "Wrong store product",
                    "source_product_id": "product-b",
                    "source_url": "https://api.example.test/products?storeCode=1535",
                    "store_code": "1535",
                },
            ],
        )
        return run_dir

    def test_ingest_writes_lineage_and_quality_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            run_dir = self.make_run_dir(base)
            manifest = ingest_run(
                run_dir,
                base / "warehouse",
                input_names=["products.jsonl"],
                ingested_at="2026-07-01T00:00:00+00:00",
            )

            records = read_jsonl(Path(manifest["bronze_output_file"]))
            quarantine = read_jsonl(Path(manifest["quarantine_output_file"]))

            self.assertEqual(manifest["records_written"], 2)
            self.assertEqual(manifest["distinct_bronze_keys"], 2)
            self.assertEqual(manifest["duplicate_bronze_keys"], 0)
            self.assertEqual(manifest["quality_status_counts"], {"QUARANTINE": 1, "WARN": 1})
            self.assertEqual(manifest["warning_counts"], {"package_size_null": 1})
            self.assertEqual(manifest["quarantine_reason_counts"], {"store_code_mismatch": 1})
            self.assertEqual(len(quarantine), 1)

            first = records[0]
            self.assertEqual(first["source_line_number"], 1)
            self.assertTrue(first["source_file"].endswith("products.jsonl"))
            self.assertEqual(first["raw_payload"]["source_product_id"], "product-a")
            self.assertEqual(first["quality_status"], "WARN")

    def test_ingest_is_idempotent_for_same_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            run_dir = self.make_run_dir(base)
            kwargs = {
                "run_dir": run_dir,
                "input_names": ["products.jsonl"],
                "ingested_at": "2026-07-01T00:00:00+00:00",
            }

            first_manifest = ingest_run(out_dir=base / "warehouse_first", **kwargs)
            second_manifest = ingest_run(out_dir=base / "warehouse_second", **kwargs)

            first_records = read_jsonl(Path(first_manifest["bronze_output_file"]))
            second_records = read_jsonl(Path(second_manifest["bronze_output_file"]))
            first_keys = {record["bronze_record_key"] for record in first_records}
            second_keys = {record["bronze_record_key"] for record in second_records}

            self.assertEqual(first_keys, second_keys)
            self.assertEqual(first_manifest["records_file_sha256"], second_manifest["records_file_sha256"])


if __name__ == "__main__":
    unittest.main()
