#!/usr/bin/env python3
import sys
from pathlib import Path
from compare_runs import compare_runs


def main():
    if len(sys.argv) not in (3, 4):
        print(
            "Usage:\n"
            "  python compare_baselines.py "
            "<baseline_snapshot_dir> "
            "<observed_snapshot_dir> "
            "[diff_output_dir]\n\n"
            "Example:\n"
            "  python compare_baselines.py "
            "data/snapshots/baselines/630 "
            "data/snapshots/observed/640 "
            "data/diffs/baseline_630__observed_640"
        )
        sys.exit(1)

    baseline_dir = Path(sys.argv[1])
    observed_dir = Path(sys.argv[2])
    output_dir = Path(sys.argv[3]) if len(sys.argv) == 4 else None

    result = compare_runs(baseline_dir, observed_dir, output_dir)

    print(f"\nADDED URLs ({len(result['added'])}):")
    for u in result["added"]:
        print(f"  + {u}")

    print(f"\nDELETED URLs ({len(result['deleted'])}):")
    for u in result["deleted"]:
        print(f"  - {u}")

    print(f"\nCHANGED URLs ({len(result['changed'])}):")
    for u in result["changed"]:
        print(f"  * {u}")

    print(f"\nUNCHANGED URLs ({len(result['unchanged'])})")


if __name__ == "__main__":
    main()
