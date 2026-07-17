import unittest

from jobs.spark.build_gold_price_snapshot_spark import (
    HUDI_PARTITION_FIELD,
    HUDI_PRECOMBINE_FIELD,
    HUDI_RECORD_KEY_FIELD,
    HUDI_TABLE_NAME,
    HUDI_TABLE_TYPE,
    hudi_write_options,
)


class SparkGoldHudiConfigTest(unittest.TestCase):
    def test_hudi_write_options_match_project_contract(self) -> None:
        options = hudi_write_options()

        self.assertEqual(options["hoodie.table.name"], HUDI_TABLE_NAME)
        self.assertEqual(options["hoodie.datasource.write.table.name"], HUDI_TABLE_NAME)
        self.assertEqual(options["hoodie.datasource.write.recordkey.field"], HUDI_RECORD_KEY_FIELD)
        self.assertEqual(options["hoodie.datasource.write.precombine.field"], HUDI_PRECOMBINE_FIELD)
        self.assertEqual(options["hoodie.datasource.write.partitionpath.field"], HUDI_PARTITION_FIELD)
        self.assertEqual(options["hoodie.datasource.write.table.type"], HUDI_TABLE_TYPE)
        self.assertEqual(options["hoodie.datasource.write.operation"], "upsert")


if __name__ == "__main__":
    unittest.main()
