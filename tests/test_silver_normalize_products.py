import json
import tempfile
import unittest
from pathlib import Path

from jobs.silver.normalize_products import normalize, normalize_selling_unit, parse_package


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def bronze_record(payload: dict, key: str = "bronze-key-1", status: str = "WARN") -> dict:
    return {
        "bronze_record_key": key,
        "run_id": "unit_silver_run",
        "retailer_id": "winmart",
        "store_code": "1682",
        "store_group_code": "1998",
        "region": "Binh Dinh",
        "source_dataset": "products_enriched",
        "source_file": "raw/store=winmart/date=2026-06-29/run_id=unit_silver_run/products_enriched.jsonl",
        "source_line_number": 1,
        "observed_at": "2026-06-29T13:50:04+07:00",
        "quality_status": status,
        "raw_payload": payload,
    }


class SilverNormalizeProductsTest(unittest.TestCase):
    def test_selling_unit_normalization_handles_accents(self) -> None:
        self.assertEqual(normalize_selling_unit("Cái"), "each")
        self.assertEqual(normalize_selling_unit("Cai"), "each")
        self.assertEqual(normalize_selling_unit("Gói"), "pack")
        self.assertEqual(normalize_selling_unit("goi"), "pack")
        self.assertEqual(normalize_selling_unit("Lốc"), "multi_pack")
        self.assertEqual(normalize_selling_unit("loc"), "multi_pack")

    def test_parse_mass_from_name(self) -> None:
        parsed = parse_package(
            {
                "product_name_raw": "CHANTE NG Chante h.hoa hong Phap 3.1kg",
                "unit_raw": "Can",
                "package_size_raw": None,
            }
        )

        self.assertEqual(parsed["measurement_type"], "mass")
        self.assertEqual(parsed["measurement_base_unit"], "g")
        self.assertEqual(parsed["measurement_base_quantity"], 3100)
        self.assertEqual(parsed["package_total_base_quantity"], 3100)
        self.assertTrue(parsed["unit_price_publishable"])

    def test_parse_multi_pack_volume(self) -> None:
        parsed = parse_package(
            {
                "product_name_raw": "Loc 6 chai nuoc ngot 330ml",
                "unit_raw": "Lốc",
                "package_size_raw": None,
            }
        )

        self.assertEqual(parsed["selling_unit_count"], 6)
        self.assertEqual(parsed["measurement_type"], "volume")
        self.assertEqual(parsed["measurement_base_unit"], "ml")
        self.assertEqual(parsed["package_total_base_quantity"], 1980)

    def test_count_based_product_uses_each(self) -> None:
        parsed = parse_package(
            {
                "product_name_raw": "Khan tam cotton",
                "unit_raw": "Cái",
                "package_size_raw": None,
            }
        )

        self.assertEqual(parsed["measurement_type"], "count")
        self.assertEqual(parsed["measurement_base_unit"], "each")
        self.assertEqual(parsed["package_total_base_quantity"], 1)
        self.assertTrue(parsed["unit_price_publishable"])

    def test_normalize_skips_quarantine_and_writes_silver(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bronze_file = base / "bronze_raw_records.jsonl"
            valid_payload = {
                "current_price": 135000,
                "currency": "VND",
                "is_on_promotion": True,
                "listed_price": 207200,
                "package_size_raw": None,
                "product_name_raw": "CHANTE NG Chante h.hoa hong Phap 3.1kg",
                "raw_product": {
                    "barcode": "8936136165974",
                    "brandName": "CHANTE",
                    "itemNo": "10274187",
                    "sku": "10274187CAN",
                    "uom": "CAN",
                    "uomName": "Can",
                },
                "source_url": "https://api.example.test/products",
                "stock_quantity": 3,
                "unit_raw": "Can",
            }
            quarantined_payload = {
                "current_price": 10000,
                "product_name_raw": "Wrong store",
                "raw_product": {"itemNo": "bad"},
                "unit_raw": "Cái",
            }
            write_jsonl(
                bronze_file,
                [
                    bronze_record(valid_payload, "bronze-key-1", "WARN"),
                    bronze_record(quarantined_payload, "bronze-key-2", "QUARANTINE"),
                ],
            )

            manifest = normalize(
                bronze_file=bronze_file,
                out_dir=base / "warehouse",
                normalized_at="2026-07-01T00:00:00+00:00",
            )

            products = read_jsonl(Path(manifest["retailer_products_file"]))
            observations = read_jsonl(Path(manifest["product_observations_file"]))

            self.assertEqual(manifest["retailer_products_written"], 1)
            self.assertEqual(manifest["product_observations_written"], 1)
            self.assertEqual(manifest["skipped_counts"], {"quarantine": 1})
            self.assertEqual(products[0]["measurement_base_quantity"], 3100)
            self.assertEqual(products[0]["measurement_base_unit"], "g")
            self.assertEqual(observations[0]["data_quality_status"], "warning")
            self.assertEqual(observations[0]["source_bronze_record_key"], "bronze-key-1")


if __name__ == "__main__":
    unittest.main()
