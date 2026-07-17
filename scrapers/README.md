# Retailer Scrapers

Each retailer has its own crawler module and keeps source-specific fields in
the output. `scrapers.common.to_bronze_payload` adds the generic fields used by
the Bronze/Silver contracts without flattening retailer-specific promotion
metadata.

## Layout

```text
scrapers/
  common.py
  bachhoaxanh/crawler.py
  go/crawler.py
  lotte/crawler.py
  mmvietnam/crawler.py
  winmart/
```

WinMart remains separate because it has a multi-step Playwright/API flow and
several related promotion scrapers.

## Quick test commands

Run from the repository root:

```bash
python scrapers/bachhoaxanh/crawler.py --max-categories 1 --max-scrolls 2 --output-dir /tmp/bhx-test
python scrapers/go/crawler.py --max-categories 1 --max-scrolls 2 --output-dir /tmp/go-test
python scrapers/lotte/crawler.py --max-pages 1 --output-dir /tmp/lotte-test
python scrapers/mmvietnam/crawler.py --max-pages 1 --output-dir /tmp/mm-test
```

The four crawlers can still be run as independent entry points for debugging,
but the recommended production path is the shared runner described below. The
runner creates the canonical `raw/store=*/date=*/run_id=*` directory before
running Bronze.

## Output contract boundary

Every written product keeps fields such as `name`, `sale_price`, `promotion`,
and retailer-specific data. It also receives generic fields including:

- `retailer_id`
- `source_product_id`
- `product_name_raw`
- `current_price`, `listed_price`, `promo_price`
- `is_price_discount`, `has_promo_mechanic`, `is_on_promotion`
- `raw_product`

Bronze remains responsible for adding `run_id`, source-file lineage, payload
hashes, quality status, and quarantine information.

## Shared runner

Use the shared runner to create the canonical raw run directory and metadata:

```bash
python scripts/run_retailer_crawlers.py --dry-run --test-limit 1
python scripts/run_retailer_crawlers.py --retailers go lottemart --test-limit 1 --continue-on-error
```

The runner writes `metadata.json` before and after each crawl. A failed
retailer is marked `failed`; with `--continue-on-error`, the other retailers
still run and receive their own status. Use `--run-id` to deliberately rerun
the same run directory.

## Full pipeline entry point

To run crawler, Bronze, Python Silver, and Spark Gold as one flow:

```bash
bash scripts/run_multi_retailer_pipeline.sh \
  --retailers go lottemart mmvietnam bachhoaxanh \
  --spark-output-format parquet \
  --continue-on-error
```

For a cheap smoke run:

```bash
bash scripts/run_multi_retailer_pipeline.sh \
  --retailers go lottemart \
  --test-limit 1 \
  --spark-output-format parquet \
  --continue-on-error
```

The pipeline keeps one state file per retailer under `.state/`. Therefore a
successful rerun of one retailer does not incorrectly skip or overwrite the
processing state of another retailer.

To include the dedicated WinMart crawler in the same command:

```bash
bash scripts/run_multi_retailer_pipeline.sh \
  --include-winmart \
  --spark-output-format hudi \
  --continue-on-error
```
