"""Distributed Word Count on Apache Spark (RDD API).

Pipeline:  textFile -> flatMap(tokenize) -> map(word, 1) -> reduceByKey(+) -> sort

Run on the cluster (from the master node):
    spark-submit --master spark://<master-ip>:7077 wordcount.py \
        --input file:///opt/spark-data/input/corpus.txt \
        --output /opt/spark-data/output/wordcount.tsv --partitions 24

--delay-ms makes every partition sleep before working. It exists only so the
failure-recovery demo has time to kill a worker while tasks are running.
"""

from __future__ import annotations

import argparse
import re
import socket
import sys
import time
from typing import Iterable, Iterator

WORD_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)*")


def tokenize(line: str) -> list[str]:
    """Lower-case words; apostrophes kept inside words (don't, o'clock)."""
    return WORD_RE.findall(line.lower())


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark Word Count")
    p.add_argument("--input", required=True, help="file:///... path present on every node")
    p.add_argument("--output", required=True, help="TSV written on the driver node")
    p.add_argument("--partitions", type=int, default=0, help="min input partitions (0 = Spark default)")
    p.add_argument("--top", type=int, default=20, help="print the N most frequent words")
    p.add_argument("--delay-ms", type=int, default=0, help="sleep per input partition (failure demo only)")
    p.add_argument("--reduce-delay-ms", type=int, default=0,
                   help="sleep per partition AFTER the shuffle (shuffle-failure demo only)")
    p.add_argument("--app-name", default="WordCount")
    return p.parse_args(argv)


def _slow_partition(delay_ms: int):
    def run(lines: Iterable[str]) -> Iterator[str]:
        time.sleep(delay_ms / 1000.0)
        yield from lines
    return run


def main(argv: list[str]) -> int:
    from pyspark.sql import SparkSession  # imported here so tokenize() is usable without Spark

    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    sc = spark.sparkContext
    started = time.time()

    lines = sc.textFile(args.input, args.partitions or None)
    if args.delay_ms > 0:
        lines = lines.mapPartitions(_slow_partition(args.delay_ms))

    counts = (
        lines.flatMap(tokenize)
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    if args.reduce_delay_ms > 0:
        # Slow the reduce side so a worker can be killed while shuffle output is being read.
        counts = counts.mapPartitions(_slow_partition(args.reduce_delay_ms))
    counts = counts.sortBy(lambda kv: (-kv[1], kv[0]))
    results = counts.collect()
    elapsed = time.time() - started

    with open(args.output, "w", encoding="utf-8", newline="\n") as fh:
        for word, count in results:
            fh.write(f"{word}\t{count}\n")

    executors = sc._jsc.sc().getExecutorMemoryStatus().keySet().size() - 1  # minus driver
    total = sum(c for _, c in results)
    print("=" * 60)
    print(f"WORDCOUNT_DONE app_id={sc.applicationId} driver={socket.gethostname()}")
    print(f"input={args.input} partitions={lines.getNumPartitions()} executors={executors}")
    print(f"total_words={total} distinct_words={len(results)} elapsed_s={elapsed:.1f}")
    print(f"output={args.output}")
    print(f"top {args.top}:")
    for word, count in results[: args.top]:
        print(f"  {count:>10}  {word}")
    print("=" * 60)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
