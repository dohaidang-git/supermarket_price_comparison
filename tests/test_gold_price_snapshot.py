import json
import tempfile
import unittest
from pathlib import Path

from jobs.gold.build_daily_price_snapshot import build_gold


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class GoldPriceSnapshotTest(unittest.TestCase):
    def test_builds_latest_daily_snapshot_and_unit_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            products_file = base / "retailer_products.jsonl"
            observations_file = base / "product_observations.jsonl"

            write_jsonl(
                products_file,
                [
                    {
                        "retailer_product_id": "p1",
                        "retailer_id": "winmart",
                        "product_name": "Water 1.5L",
                        "brand": "Brand",
                        "category_raw": "Drink",
                        "unit_price_publishable": True,
                        "package_total_base_quantity": 1500,
                        "measurement_base_unit": "ml",
                        "measurement_type": "volume",
                    },
                    {
                        "retailer_product_id": "p2",
                        "retailer_id": "winmart",
                        "product_name": "Unknown pack",
                        "brand": "Brand",
                        "category_raw": "Care",
                        "unit_price_publishable": False,
                        "package_total_base_quantity": 1,
                        "measurement_base_unit": "each",
                        "measurement_type": "count",
                    },
                ],
            )
            write_jsonl(
                observations_file,
                [
                    {
                        "observation_id": "old",
                        "retailer_product_id": "p1",
                        "retailer_id": "winmart",
                        "store_code": "1682",
                        "store_group_code": "1998",
                        "region": "Binh Dinh",
                        "observed_at": "2026-06-29T10:00:00+07:00",
                        "observation_date": "2026-06-29",
                        "listed_price": 50000,
                        "promo_price": 45000,
                        "current_price": 45000,
                        "currency": "VND",
                        "discount_amount": 5000,
                        "discount_percent": 10,
                        "availability_status": "in_stock",
                        "is_on_promotion": True,
                        "is_price_discount": True,
                        "has_promo_mechanic": False,
                        "source_run_id": "unit_gold_run",
                        "source_bronze_record_key": "bronze-old",
                        "data_quality_status": "valid",
                    },
                    {
                        "observation_id": "new",
                        "retailer_product_id": "p1",
                        "retailer_id": "winmart",
                        "store_code": "1682",
                        "store_group_code": "1998",
                        "region": "Binh Dinh",
                        "observed_at": "2026-06-29T12:00:00+07:00",
                        "observation_date": "2026-06-29",
                        "listed_price": 50000,
                        "promo_price": 42000,
                        "current_price": 42000,
                        "currency": "VND",
                        "discount_amount": 8000,
                        "discount_percent": 16,
                        "availability_status": "in_stock",
                        "is_on_promotion": True,
                        "is_price_discount": True,
                        "has_promo_mechanic": False,
                        "source_run_id": "unit_gold_run",
                        "source_bronze_record_key": "bronze-new",
                        "data_quality_status": "valid",
                    },
                    {
                        "observation_id": "p2obs",
                        "retailer_product_id": "p2",
                        "retailer_id": "winmart",
                        "store_code": "1682",
                        "store_group_code": "1998",
                        "region": "Binh Dinh",
                        "observed_at": "2026-06-29T12:00:00+07:00",
                        "observation_date": "2026-06-29",
                        "listed_price": 30000,
                        "promo_price": 25000,
                        "current_price": 25000,
                        "currency": "VND",
                        "discount_amount": 5000,
                        "discount_percent": 16.6667,
                        "availability_status": "unknown",
                        "is_on_promotion": True,
                        "is_price_discount": True,
                        "has_promo_mechanic": False,
                        "source_run_id": "unit_gold_run",
                        "source_bronze_record_key": "bronze-p2",
                        "data_quality_status": "warning",
                    },
                ],
            )

            manifest = build_gold(
                products_file=products_file,
                observations_file=observations_file,
                out_dir=base / "warehouse",
                built_at="2026-07-02T00:00:00+00:00",
            )

            rows = read_jsonl(Path(manifest["price_snapshot_file"]))
            by_product = {row["retailer_product_id"]: row for row in rows}

            self.assertEqual(manifest["snapshots_written"], 2)
            self.assertEqual(manifest["selection_stats"]["duplicate_observation_groups"], 1)
            self.assertEqual(by_product["p1"]["observation_id"], "new")
            self.assertEqual(by_product["p1"]["effective_unit_price"], 28)
            self.assertEqual(by_product["p1"]["comparison_unit"], "ml")
            self.assertIsNone(by_product["p2"]["effective_unit_price"])
            self.assertIsNone(by_product["p2"]["comparison_unit"])
            self.assertEqual(manifest["validation_status"], "passed")
            self.assertEqual(manifest["block_publish_issues"], 0)
            self.assertGreater(manifest["warn_issues"], 0)


if __name__ == "__main__":
    unittest.main()
