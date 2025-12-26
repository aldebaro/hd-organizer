"""
Analyze the largest duplicate files and pairs of folders containing duplicates.

This script:
- Loads the file index from a pickle file
- Identifies the N largest duplicate file groups
- Finds pairs of folders that contain duplicates
- Sorts folder pairs by total duplicated data
"""

import sys
import pickle
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# Try to import tqdm for progress bar
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None  # type: ignore


class DuplicateAnalyzer:
    """Analyze duplicates and folder pairs from file index."""

    def __init__(self, pickle_file: str):
        """
        Initialize with a pickle file containing the file index.

        Args:
            pickle_file: Path to the pickle file from name_size_dup_step1.py
        """
        self.file_index = self._load_pickle(pickle_file)

    @staticmethod
    def _load_pickle(filepath: str) -> Dict:
        """Load the file index from pickle file."""
        try:
            with open(filepath, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            print(f"Error: Pickle file '{filepath}' not found")
            sys.exit(1)

    def get_all_duplicates(self) -> Dict[Tuple[str, str, int], List[str]]:
        """
        Get all files that have duplicates (same name, extension, and size).

        Returns:
            Dictionary of only entries with multiple paths
        """
        return {k: v for k, v in self.file_index.items() if len(v) > 1}

    def get_largest_duplicates(self, n: int) -> List[Tuple[Tuple[str, str, int], List[str], int]]:
        """
        Get the N largest duplicate file groups.

        Args:
            n: Number of largest duplicates to return

        Returns:
            List of tuples: ((filename, extension, size), paths, total_wasted_bytes)
            Sorted by total wasted space in descending order
        """
        duplicates = self.get_all_duplicates()

        # Calculate wasted space for each duplicate group
        duplicate_groups = []
        for (filename, ext, size), paths in duplicates.items():
            if len(paths) > 1:
                # Wasted space = size * (number_of_copies - 1)
                wasted_bytes = size * (len(paths) - 1)
                duplicate_groups.append(((filename, ext, size), paths, wasted_bytes))

        # Sort by wasted space in descending order
        duplicate_groups.sort(key=lambda x: x[2], reverse=True)

        return duplicate_groups[:n]

    def get_folder_pairs(self) -> Dict[Tuple[str, str], int]:
        """
        Find pairs of folders that contain duplicate files and sum their duplicated data.

        Returns:
            Dictionary mapping folder pairs to total duplicated bytes
        """
        duplicates = self.get_all_duplicates()
        folder_pairs_data = defaultdict(int)

        # Iterate over all duplicate groups
        for (filename, ext, size), paths in duplicates.items():
            if len(paths) <= 1:
                continue

            # Get parent folders for each duplicate
            folders = []
            for path in paths:
                folder = str(Path(path).parent)
                if folder not in folders:
                    folders.append(folder)

            # Create pairs from folders (all unique combinations)
            for i in range(len(folders)):
                for j in range(i + 1, len(folders)):
                    folder1 = folders[i]
                    folder2 = folders[j]

                    # Normalize pair order for consistent keys
                    pair = tuple(sorted([folder1, folder2]))

                    # Add the size of one copy (wasted space for this pair)
                    folder_pairs_data[pair] += size

        return dict(folder_pairs_data)

    def print_largest_duplicates(self, n: int) -> None:
        """
        Print the N largest duplicate file groups with formatted output.

        Args:
            n: Number of largest duplicates to display
        """
        largest = self.get_largest_duplicates(n)

        print("\n" + "=" * 80)
        print(f"TOP {n} LARGEST DUPLICATE FILE GROUPS")
        print("=" * 80)

        if not largest:
            print("No duplicates found.")
            return

        total_wasted = 0
        for idx, ((filename, ext, size), paths, wasted_bytes) in enumerate(largest, 1):
            ext_display = f".{ext}" if ext else ""
            total_wasted += wasted_bytes

            print(f"\n{idx}. {filename}{ext_display}")
            print(f"   File size: {self._format_bytes(size)}")
            print(f"   Number of copies: {len(paths)}")
            print(f"   Wasted space: {self._format_bytes(wasted_bytes)}")
            print(f"   Locations:")

            for path in sorted(paths):
                print(f"     - {path}")

        print("\n" + "=" * 80)
        print(f"Total wasted space in top {n}: {self._format_bytes(total_wasted)}")
        print("=" * 80)

    def print_folder_pairs(self, top_n: int = 20) -> None:
        """
        Print folder pairs sorted by total duplicated data.

        Args:
            top_n: Number of top folder pairs to display
        """
        pairs = self.get_folder_pairs()

        # Sort by total duplicated bytes in descending order
        sorted_pairs = sorted(pairs.items(), key=lambda x: x[1], reverse=True)

        print("\n" + "=" * 90)
        print(f"TOP {top_n} FOLDER PAIRS WITH DUPLICATES")
        print("=" * 90)

        if not sorted_pairs:
            print("No folder pairs with duplicates found.")
            return

        total_all_pairs = sum(bytes_count for _, bytes_count in sorted_pairs)

        print(f"\n{'Rank':<6} {'Duplicated Data':<20} {'Folder 1':<35} {'Folder 2':<35}")
        print("-" * 90)

        for idx, ((folder1, folder2), total_bytes) in enumerate(sorted_pairs[:top_n], 1):
            # Truncate long folder paths for display
            folder1_display = folder1 if len(folder1) <= 32 else "..." + folder1[-29:]
            folder2_display = folder2 if len(folder2) <= 32 else "..." + folder2[-29:]

            print(
                f"{idx:<6} {self._format_bytes(total_bytes):<20} {folder1_display:<35} {folder2_display:<35}")

        print("-" * 90)
        print(
            f"Total duplicated data across all folder pairs: {self._format_bytes(total_all_pairs)}")
        print("=" * 90)

    def print_full_folder_pairs(self, top_n: int = 20) -> None:
        """
        Print folder pairs with full paths (not truncated).

        Args:
            top_n: Number of top folder pairs to display
        """
        pairs = self.get_folder_pairs()

        # Sort by total duplicated bytes in descending order
        sorted_pairs = sorted(pairs.items(), key=lambda x: x[1], reverse=True)

        print("\n" + "=" * 100)
        print(f"TOP {top_n} FOLDER PAIRS WITH DUPLICATES (FULL PATHS)")
        print("=" * 100)

        if not sorted_pairs:
            print("No folder pairs with duplicates found.")
            return

        total_all_pairs = sum(bytes_count for _, bytes_count in sorted_pairs)

        for idx, ((folder1, folder2), total_bytes) in enumerate(sorted_pairs[:top_n], 1):
            print(f"\n{idx}. Duplicated Data: {self._format_bytes(total_bytes)}")
            print(f"   Folder 1: {folder1}")
            print(f"   Folder 2: {folder2}")

        print("\n" + "=" * 100)
        print(
            f"Total duplicated data across all folder pairs: {self._format_bytes(total_all_pairs)}")
        print("=" * 100)

    @staticmethod
    def _format_bytes(bytes_count: int) -> str:
        """Format bytes to human-readable format."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024:
                return f"{bytes_count:.2f} {unit}"
            bytes_count /= 1024
        return f"{bytes_count:.2f} PB"


def print_help():
    """Print help message."""
    help_text = """
Usage: python analyze_largest_duplicates.py [OPTIONS]

Description:
    Analyze the largest duplicate files and folder pairs containing duplicates.
    Reads the file index from a pickle file and generates reports.

Options:
    --pickle <file>          Path to the pickle file (default: index.pkl)
    --largest <N>            Show top N largest duplicate file groups (default: 10)
    --folder-pairs <N>       Show top N folder pairs with duplicates (default: 20)
    --full-paths             Display folder pairs with full paths (not truncated)
    --help                   Show this help message and exit

Examples:
    python analyze_largest_duplicates.py
    python analyze_largest_duplicates.py --pickle custom.pkl --largest 20
    python analyze_largest_duplicates.py --largest 5 --folder-pairs 10
    python analyze_largest_duplicates.py --folder-pairs 50 --full-paths
    """
    print(help_text)


def main():
    """Main function to analyze duplicates."""
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    # Parse command line arguments
    pickle_file = "index.pkl"
    largest_n = 10
    folder_pairs_n = 20
    full_paths = False

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == "--pickle" and i + 1 < len(sys.argv):
            pickle_file = sys.argv[i + 1]
            i += 2
        elif arg == "--largest" and i + 1 < len(sys.argv):
            try:
                largest_n = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Error: --largest argument must be an integer")
                sys.exit(1)
        elif arg == "--folder-pairs" and i + 1 < len(sys.argv):
            try:
                folder_pairs_n = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Error: --folder-pairs argument must be an integer")
                sys.exit(1)
        elif arg == "--full-paths":
            full_paths = True
            i += 1
        elif arg.startswith("--"):
            print(f"Error: Unknown option '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)

    # Load and analyze
    print(f"Loading file index from: {pickle_file}")
    analyzer = DuplicateAnalyzer(pickle_file)

    # Display results
    analyzer.print_largest_duplicates(largest_n)

    if full_paths:
        analyzer.print_full_folder_pairs(folder_pairs_n)
    else:
        analyzer.print_folder_pairs(folder_pairs_n)


if __name__ == "__main__":
    main()
