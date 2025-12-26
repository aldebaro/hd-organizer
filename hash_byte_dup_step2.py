"""
Load the pickle file and find duplicate files based on
their names and sizes.

This script:
- Loads the file index from a previously created pickle file
- Finds groups of files with identical name and size
- Implements two methods to compare pairs of files:
  1. Efficient byte-by-byte comparison
  2. Hash-based comparison using SHA-256
- Identifies true duplicates and reports their locations
"""

import sys
import pickle
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Any

# Try to import tqdm for progress bar, fallback if not available
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None  # type: ignore


class DuplicateFinder:
    """Find true duplicate files using multiple comparison methods."""

    def __init__(self, pickle_file: str, min_file_size: int = 0):
        """
        Initialize with a pickle file containing the file index.

        Args:
            pickle_file: Path to the pickle file from all_file_names.py
            min_file_size: Minimum file size in bytes to consider as duplicate (default: 0, no minimum)
        """
        self.file_index = self._load_pickle(pickle_file)
        self.hash_cache: Dict[str, str] = {}
        self.min_file_size = min_file_size

    @staticmethod
    def _load_pickle(filepath: str) -> Dict:
        """Load the file index from pickle file."""
        try:
            with open(filepath, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            print(f"Error: Pickle file '{filepath}' not found")
            sys.exit(1)

    def find_candidates_by_name_and_size(self) -> Dict[Tuple[str, str, int], List[str]]:
        """
        Find all groups of files with identical name, extension, and size.

        Only includes files that meet the minimum file size requirement.

        Returns:
            Dictionary of (filename, extension, size) -> list of paths
        """
        candidates = {}
        for (fname, ext, size), paths in self.file_index.items():
            # Skip files smaller than minimum size
            if size < self.min_file_size:
                continue

            # Only include groups with more than one file
            if len(paths) > 1:
                candidates[(fname, ext, size)] = paths
        return candidates

    @staticmethod
    def compare_files_byte_by_byte(file1: str, file2: str, chunk_size: int = 8192) -> bool:
        """
        Compare two files byte-by-byte efficiently.

        Reads files in chunks to minimize memory usage for large files.
        Stops reading as soon as a difference is found.

        Args:
            file1: Path to first file
            file2: Path to second file
            chunk_size: Size of chunks to read at once (default 8KB)

        Returns:
            True if files are identical, False otherwise
        """
        try:
            with open(file1, "rb") as f1, open(file2, "rb") as f2:
                while True:
                    chunk1 = f1.read(chunk_size)
                    chunk2 = f2.read(chunk_size)

                    # If chunks differ, files are not identical
                    if chunk1 != chunk2:
                        return False

                    # End of file reached
                    if not chunk1:
                        return True
        except (OSError, PermissionError):
            return False

    def compute_file_hash(self, filepath: str, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
        """
        Compute hash of a file using the specified algorithm.

        Args:
            filepath: Path to the file
            algorithm: Hash algorithm ('sha256', 'md5', 'sha1', etc.)
            chunk_size: Size of chunks to read at once

        Returns:
            Hexadecimal hash string
        """
        # Return cached hash if available
        if filepath in self.hash_cache:
            return self.hash_cache[filepath]

        try:
            hasher = hashlib.new(algorithm)
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hasher.update(chunk)

            hash_value = hasher.hexdigest()
            self.hash_cache[filepath] = hash_value
            return hash_value
        except (OSError, PermissionError):
            return ""

    def compare_files_by_hash(self, file1: str, file2: str) -> bool:
        """
        Compare two files using SHA-256 hash comparison.

        Args:
            file1: Path to first file
            file2: Path to second file

        Returns:
            True if files have identical SHA-256 hashes, False otherwise
        """
        hash1 = self.compute_file_hash(file1, "sha256")
        hash2 = self.compute_file_hash(file2, "sha256")

        if not hash1 or not hash2:
            return False

        return hash1 == hash2

    def find_true_duplicates_byte_comparison(self) -> Dict[Tuple[str, str, int], List[List[str]]]:
        """
        Find true duplicates using byte-by-byte comparison.

        Returns:
            Dictionary mapping (filename, extension, size) to groups of identical files
        """
        candidates = self.find_candidates_by_name_and_size()
        true_duplicates = {}

        items = list(candidates.items())
        iterator = tqdm(items, desc="Comparing (byte)", unit=" group",
                        disable=not HAS_TQDM) if HAS_TQDM else items

        for key, paths in iterator:
            groups = self._group_identical_files_byte(paths)
            if groups:
                true_duplicates[key] = groups

        return true_duplicates

    def find_true_duplicates_hash_comparison(self) -> Dict[Tuple[str, str, int], List[List[str]]]:
        """
        Find true duplicates using SHA-256 hash comparison.

        Returns:
            Dictionary mapping (filename, extension, size) to groups of identical files
        """
        candidates = self.find_candidates_by_name_and_size()
        true_duplicates = {}

        items = list(candidates.items())
        iterator = tqdm(items, desc="Comparing (hash)", unit=" group",
                        disable=not HAS_TQDM) if HAS_TQDM else items

        for key, paths in iterator:
            groups = self._group_identical_files_hash(paths)
            if groups:
                true_duplicates[key] = groups

        return true_duplicates

    @staticmethod
    def _group_identical_files_byte(paths: List[str]) -> List[List[str]]:
        """Group files that are identical by byte-by-byte comparison."""
        if not paths:
            return []

        groups = []
        remaining = set(paths)

        while remaining:
            # Start a new group with the first remaining file
            current_file = remaining.pop()
            current_group = [current_file]

            # Find all files identical to current_file
            to_remove = set()
            for other_file in remaining:
                if DuplicateFinder.compare_files_byte_by_byte(current_file, other_file):
                    current_group.append(other_file)
                    to_remove.add(other_file)

            remaining -= to_remove
            if len(current_group) > 1:  # Only include groups with duplicates
                groups.append(current_group)

        return groups

    def _group_identical_files_hash(self, paths: List[str]) -> List[List[str]]:
        """Group files that are identical by hash comparison."""
        if not paths:
            return []

        # Create hash -> files mapping
        hash_groups: Dict[str, List[str]] = {}
        for filepath in paths:
            file_hash = self.compute_file_hash(filepath)
            if file_hash:  # Skip files that couldn't be hashed
                if file_hash not in hash_groups:
                    hash_groups[file_hash] = []
                hash_groups[file_hash].append(filepath)

        # Return only groups with duplicates (more than 1 file)
        return [group for group in hash_groups.values() if len(group) > 1]

    def print_duplicate_report(self, true_duplicates: Dict, method: str = "unknown") -> None:
        """
        Print a formatted report of duplicate files.

        Args:
            true_duplicates: Dictionary from find_true_duplicates methods
            method: Name of the comparison method used
        """
        if not true_duplicates:
            print(f"\nNo true duplicates found using {method} method.")
            return

        total_duplicate_groups = 0
        total_duplicate_files = 0
        total_wasted_space = 0

        print(f"\n{'='*70}")
        print(f"True Duplicates Found Using {method.upper()} Comparison")
        print(f"{'='*70}\n")

        for (fname, ext, size), groups in sorted(true_duplicates.items()):
            ext_display = ext if ext else "(no extension)"
            print(f"File: {fname}.{ext_display} ({size} bytes)")

            for group_idx, group in enumerate(groups, 1):
                print(f"  Group {group_idx} - {len(group)} copies:")
                for filepath in sorted(group):
                    print(f"    {filepath}")

                # Calculate wasted space (all copies except the first one)
                wasted = size * (len(group) - 1)
                total_wasted_space += wasted
                total_duplicate_groups += 1
                total_duplicate_files += len(group)
                print(f"    Wasted space: {wasted:,} bytes")

            print()

        print(f"{'='*70}")
        print(f"Summary:")
        print(f"  Total duplicate groups: {total_duplicate_groups}")
        print(f"  Total duplicate files: {total_duplicate_files}")
        print(
            f"  Total wasted space: {total_wasted_space:,} bytes ({total_wasted_space / (1024**2):.2f} MB)")
        print(f"{'='*70}\n")

    def save_results_to_json(self, true_duplicates: Dict, differences: Dict, output_duplicates: str, output_differences: str) -> Tuple[int, int]:
        """
        Save duplicate groups and differences to JSON files.

        For duplicates, includes modification dates for each file to support
        post-processing that keeps only the newest copy.

        Args:
            true_duplicates: Dictionary of true duplicate groups
            differences: Dictionary of files that differ
            output_duplicates: Path to output file for duplicates
            output_differences: Path to output file for differences

        Returns:
            Tuple of (duplicate_groups_count, difference_groups_count)
        """
        # Helper function to get file modification date
        def get_file_date(filepath: str) -> str:
            """Get file modification date in ISO format."""
            try:
                mtime = Path(filepath).stat().st_mtime
                return datetime.fromtimestamp(mtime).isoformat()
            except (OSError, FileNotFoundError):
                return ""

        # Prepare duplicates data
        duplicates_data = []
        for (fname, ext, size), groups in sorted(true_duplicates.items()):
            ext_display = ext if ext else ""
            for group_idx, group in enumerate(groups, 1):
                sorted_group = sorted(group)
                dates = [get_file_date(path) for path in sorted_group]

                duplicates_data.append({
                    "filename": fname,
                    "extension": ext_display,
                    "size_bytes": size,
                    "group_id": group_idx,
                    "file_count": len(sorted_group),
                    "paths": sorted_group,
                    "dates": dates,
                    "newest_index": dates.index(max(dates)) if dates and any(dates) else 0,
                    "wasted_space_bytes": size * (len(sorted_group) - 1)
                })

        # Save duplicates to JSON
        with open(output_duplicates, "w", encoding="utf-8") as f:
            json.dump({
                "method": "hash-based and byte-by-byte comparison",
                "total_groups": len(duplicates_data),
                "total_wasted_space_bytes": sum(item["wasted_space_bytes"] for item in duplicates_data),
                "note": "dates list corresponds to paths list (same order and length). newest_index indicates which file is most recent.",
                "duplicates": duplicates_data
            }, f, indent=2, ensure_ascii=False)

        print(
            f"Duplicate groups saved to: {output_duplicates} ({len(duplicates_data)} groups)")

        # Prepare differences data
        differences_data = []
        for (fname, ext, size), groups in sorted(differences.items()):
            ext_display = ext if ext else ""
            for group_idx, group in enumerate(groups, 1):
                sorted_group = sorted(group)
                dates = [get_file_date(path) for path in sorted_group]

                differences_data.append({
                    "filename": fname,
                    "extension": ext_display,
                    "size_bytes": size,
                    "group_id": group_idx,
                    "file_count": len(sorted_group),
                    "paths": sorted_group,
                    "dates": dates,
                    "newest_index": dates.index(max(dates)) if dates and any(dates) else 0
                })

        # Save differences to JSON
        with open(output_differences, "w", encoding="utf-8") as f:
            json.dump({
                "method": "hash-based and byte-by-byte comparison",
                "total_groups": len(differences_data),
                "description": "Files with same name/extension/size but different content",
                "note": "dates list corresponds to paths list (same order and length). newest_index indicates which file is most recent.",
                "differences": differences_data
            }, f, indent=2, ensure_ascii=False)

        print(
            f"Difference groups saved to: {output_differences} ({len(differences_data)} groups)")

        return len(duplicates_data), len(differences_data)


def print_help():
    """Print help message."""
    help_text = """
Usage: python hash_byte_dup_step2.py <pickle_file> [OPTIONS]

Description:
    Find true duplicate files using hash-based or byte-by-byte comparison.
    Outputs two JSON files: one for true duplicates and one for files with
    identical name/size but different content.

Arguments:
    pickle_file              Path to the pickle file from name_size_dup_step1.py

Options:
    --method <method>        Comparison method: 'byte' or 'hash' (default: hash)
    --min-size <bytes>       Minimum file size in bytes to consider (default: 0)
    --output-prefix <prefix> Output file prefix (default: duplicates_results)
    --help                   Show this help message and exit

Examples:
    python hash_byte_dup_step2.py index.pkl
    python hash_byte_dup_step2.py index.pkl --method hash
    python hash_byte_dup_step2.py index.pkl --min-size 1048576
    python hash_byte_dup_step2.py index.pkl --min-size 1048576 --output-prefix results
    python hash_byte_dup_step2.py index.pkl --method byte --output-prefix my_results

Notes:
    - byte method: Slower but more thorough comparison
    - hash method: Faster using SHA-256 hashing (default)
    - --min-size is in bytes (e.g., 1048576 = 1MB)
"""
    print(help_text)


def main():
    """Main function to find and report duplicates."""
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0 if "--help" in sys.argv or "-h" in sys.argv else 1)

    pickle_file = sys.argv[1]
    method = "hash"  # Default to hash method (faster)
    output_prefix = "duplicates_results"
    min_file_size = 0  # Default: no minimum size

    # Parse optional arguments
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == "--method" and i + 1 < len(sys.argv):
            method = sys.argv[i + 1].lower()
            i += 2
        elif arg == "--min-size" and i + 1 < len(sys.argv):
            try:
                min_file_size = int(sys.argv[i + 1])
            except ValueError:
                print(f"Error: --min-size must be an integer (bytes)")
                print("Use --help for usage information")
                sys.exit(1)
            i += 2
        elif arg == "--output-prefix" and i + 1 < len(sys.argv):
            output_prefix = sys.argv[i + 1]
            i += 2
        elif arg.startswith("--"):
            print(f"Error: Unknown option '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)

    if method not in ["byte", "hash"]:
        print(f"Error: Unknown method '{method}'. Use 'byte' or 'hash'")
        print("Use --help for usage information")
        sys.exit(1)

    print(f"Loading file index from: {pickle_file}")
    finder = DuplicateFinder(pickle_file, min_file_size)

    if min_file_size > 0:
        size_mb = min_file_size / (1024 * 1024)
        print(f"Minimum file size: {min_file_size} bytes ({size_mb:.2f} MB)")

    # Find candidates first
    candidates = finder.find_candidates_by_name_and_size()
    print(f"Found {len(candidates)} groups with same name, extension, and size (meeting minimum size requirement)")
    print(
        f"Total candidate files: {sum(len(paths) for paths in candidates.values())}\n")

    # Run selected comparison method
    if method == "byte":
        print("Running BYTE-BY-BYTE comparison...")
        true_dups = finder.find_true_duplicates_byte_comparison()
    else:  # hash
        print("Running HASH-BASED comparison...")
        true_dups = finder.find_true_duplicates_hash_comparison()

    finder.print_duplicate_report(true_dups, method.upper())

    # Find files that differ (candidates that are not duplicates)
    differences = {}
    for key, paths in candidates.items():
        # Check if this key is in true_dups
        if key not in true_dups:
            # These files have same name/size but different content
            differences[key] = [[path] for path in paths]
        else:
            # Find any groups not in true_dups (shouldn't happen with current logic)
            # but handle edge cases
            found_paths = set()
            for group in true_dups[key]:
                found_paths.update(group)

            remaining = [p for p in paths if p not in found_paths]
            if remaining:
                differences[key] = [[path] for path in remaining]

    # Save results to JSON files
    output_dups_file = f"{output_prefix}_duplicates.json"
    output_diff_file = f"{output_prefix}_differences.json"

    dup_groups_count, diff_groups_count = finder.save_results_to_json(true_dups, differences,
                                                                      output_dups_file, output_diff_file)

    print(f"\nResults saved successfully!")


if __name__ == "__main__":
    main()
