"""Single-machine reference Word Count, used to check the cluster's answer.

    python jobs/local_count.py corpus.txt > local.tsv
Output format and ordering match wordcount.py exactly (count desc, word asc).
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wordcount import tokenize  # noqa: E402


def count_file(path: Path) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            counts.update(tokenize(line))
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: local_count.py FILE", file=sys.stderr)
        return 2
    out = sys.stdout
    out.reconfigure(newline="\n")  # byte-identical to the cluster output, even on Windows
    for word, count in count_file(Path(argv[0])):
        out.write(f"{word}\t{count}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
