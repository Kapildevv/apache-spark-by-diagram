# Apache Spark 4 by Diagram: companion code

The runnable code and data for the book *Apache Spark 4 by Diagram*, by Kapil Dev V. Every
listing in the book is here and runs on PySpark 4.3.0 with Python 3.14.7 and JDK 21, pinned in
`uv.lock`. The running case is **Saffron Lane**, a fictional online marketplace in India; its
data is synthetic (see [data/README.md](data/README.md)).

## Start

You need JDK 21, [uv](https://docs.astral.sh/uv/) and git, on Linux or macOS. On Windows, run
everything inside WSL 2 with Ubuntu (Spark's file writes fail on native Windows), and clone into
your Linux home folder, not under `/mnt/c`. Chapter 1 of the book walks through each step.

```bash
git clone https://github.com/Kapildevv/apache-spark-by-diagram.git
cd apache-spark-by-diagram
uv sync
uv run python chapters/ch01/first_app.py
```

## What is where

| Folder | What |
|---|---|
| `chapters/ch01` to `chapters/ch14` | the listings of each chapter |
| `solutions/` | worked solutions to the Practice items, named after the item (`ch07_anti_semi.py` is P7.2) |
| `apps/monthly_report.py` | chapter 14's application, run with `spark-submit` |
| `saffron/` | the small helpers the chapters import (`session.start`, `read_table`, `latest_orders`, ...) |
| `data/raw/day1`, `data/raw/day2` | two nights' exports of Saffron Lane's five tables |

Scripts write to `output/` and `spark-warehouse/` inside this folder; both can be deleted at any time.

## Licence

The code and the synthetic data are released under the MIT License (see [LICENSE](LICENSE)).
The book's text and figures are not: they remain all rights reserved.
