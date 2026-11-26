"""Build a large text file for Word Count demos.

    python jobs/make_corpus.py --out corpus.txt --size-mb 200
    python jobs/make_corpus.py --source book.txt --out corpus.txt --size-mb 500

Without --source, a deterministic synthetic text is generated (fixed seed,
Zipf-like word frequencies), so every run produces the same file and the
same expected counts. With --source, that text is repeated to the target size.
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

SEED = 42
WORDS_PER_LINE = 12
VOCAB = (
    "the of and to in a is that for it as was with be by on not he i this are or his from at which "
    "but have an they you were her she there been one all we their has would when if so no will "
    "spark cluster data node worker master driver executor task stage job partition shuffle memory "
    "disk network failure recovery lineage resilient distributed dataset reduce map count word "
    "apache hadoop scala python java machine system parallel compute scale fault tolerance big "
    "analytics stream batch query engine core physical lab project group evaluation demo result"
).split()


def synthetic_lines(rng: random.Random):
    weights = [1.0 / (rank + 1) for rank in range(len(VOCAB))]  # Zipf-like
    while True:
        yield " ".join(rng.choices(VOCAB, weights=weights, k=WORDS_PER_LINE)) + "\n"


def source_lines(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    if not text:
        raise SystemExit(f"{path} is empty")
    while True:
        yield from text


def build(out: Path, size_mb: int, source: Path | None) -> int:
    target = size_mb * 1024 * 1024
    lines = source_lines(source) if source else synthetic_lines(random.Random(SEED))
    written = 0
    with out.open("w", encoding="utf-8", newline="\n") as fh:
        for line in lines:
            fh.write(line)
            written += len(line.encode("utf-8"))
            if written >= target:
                break
    return written


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--size-mb", type=int, default=100)
    p.add_argument("--source", type=Path)
    args = p.parse_args(argv)
    written = build(args.out, args.size_mb, args.source)
    print(f"wrote {args.out} ({written / 1024 / 1024:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
