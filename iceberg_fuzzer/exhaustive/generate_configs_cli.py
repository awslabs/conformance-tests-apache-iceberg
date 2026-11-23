#!/usr/bin/env python3
"""
CLI Entry Point for Table Configuration Generator

Usage:
    %(prog)s --output configs.jsonl
    %(prog)s --max-rows 3 --output test.jsonl --limit 100
    %(prog)s --progress --output all_configs.jsonl
"""

import argparse
import json
import sys
import traceback
from pathlib import Path
from .shape_config_generator import TableConfigGenerator


def main():
    parser = argparse.ArgumentParser(
        description='Generate table configurations for Iceberg compaction testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default mode: Equivalence-based (recommended)
  %(prog)s --output configs.jsonl --progress

  # Exhaustive mode with small limit (for testing)
  %(prog)s --exhaustive --limit 3 --output test.jsonl

  # Generate for rows 1-3 only
  %(prog)s --max-rows 3 --output small.jsonl

  # Custom random seed
  %(prog)s --seed 123 --output configs.jsonl

Modes:
  Default (equivalence-based):
    - Computes equivalence classes for L1+L2+L3+L4 configs
    - Representatives: Full exhaustive snapshot orderings
    - Non-representatives: Random sample (1 ordering)
    - Result: ~300K-500K configs with full coverage

  --exhaustive:
    - All configs get full exhaustive snapshot orderings
    - ≤5 files: All ordered partitions (100s of orderings)
    - ≥6 files: The same orderings as for five or fewer files, collapsed
    - Result: 2M+ configs
    - Use with --limit for testing only
        """
    )

    parser.add_argument(
        '--max-rows',
        type=int,
        default=6,
        choices=range(1, 7),
        metavar='N',
        help='Maximum row count (1-6, default: 6)'
    )

    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Output file path (JSONL format)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of configurations to generate'
    )

    parser.add_argument(
        '--progress',
        action='store_true',
        help='Show progress updates every 10,000 configs'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print first few configs for debugging'
    )

    parser.add_argument(
        '--exhaustive',
        action='store_true',
        help='Use exhaustive snapshot generation for all configs (instead of equivalence-based)'
    )

    args = parser.parse_args()

    # Validate exhaustive mode
    if args.exhaustive and args.limit and args.limit > 5:
        print("⚠️  WARNING: --exhaustive with --limit > 5 may generate many configs!")
        print(f"   Each config can generate 100s-1000s of snapshot orderings.")
        print(f"   Consider using --limit 1-5 for testing with --exhaustive mode.")
        print()

    # Validate output path
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize generator
    mode = "exhaustive" if args.exhaustive else "equivalence-based"
    print(f"Initializing generator:")
    print(f"  max_rows: {args.max_rows}")
    print(f"  seed: {args.seed}")
    print(f"  snapshot mode: {mode}")
    generator = TableConfigGenerator(
        max_row_count=args.max_rows,
        seed=args.seed,
        exhaustive_mode=args.exhaustive
    )

    # Generate configs
    print(f"Generating configurations...")
    print(f"Output: {output_path}")
    if args.limit:
        print(f"Limit: {args.limit} configurations")
    print()

    count = 0
    from_representatives = 0
    try:
        with open(output_path, 'w') as f:
            for config in generator.generate_all():
                # Write to file
                f.write(json.dumps(config) + '\n')
                count += 1
                if config.get('is_representative', False):
                    from_representatives += 1

                # Progress updates
                if args.progress and count % 10000 == 0:
                    print(f"Generated {count:,} configs...")

                # Verbose: print first few
                if args.verbose and count <= 3:
                    print(f"\n--- Config {count} ---")
                    print(json.dumps(config, indent=2))

                # Limit check
                if args.limit and count >= args.limit:
                    break

    except KeyboardInterrupt:
        print(f"\n\nInterrupted by user at {count:,} configurations")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total configurations: {count:,}")
    print(f"  From representatives: {from_representatives:,} ({100*from_representatives/count:.1f}%)")
    print(f"  From non-representatives: {count - from_representatives:,} ({100*(count - from_representatives)/count:.1f}%)")
    print(f"Output file: {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    print()

    # Equivalence class statistics
    if not args.exhaustive and generator.equiv_tracker:
        tracker = generator.equiv_tracker
        print("Equivalence-based statistics:")
        print(f"  L4 representatives: {tracker.representative_count:,}")
        print(f"  L4 non-representatives: {tracker.non_representative_count:,}")
        print(f"  Equivalence classes: {len(tracker.seen_classes):,}")
        if tracker.representative_count > 0:
            ratio = tracker.non_representative_count / tracker.representative_count
            print(f"  Ratio: {ratio:.1f} non-reps per representative")
        print()

    # Quick stats
    if count > 0:
        avg_size = output_path.stat().st_size / count
        print(f"Average config size: {avg_size:.0f} bytes")
        print()

    print("Done!")


if __name__ == '__main__':
    main()
