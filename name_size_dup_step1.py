"""
Create an efficient data structure for mapping (filename, extension, size) to file paths.

This script:
- Recursively scans all files in a given directory
- Creates a nested dictionary mapping (filename, extension, size) to a list of full paths
- Allows quick lookup of all locations of identical files
- Ignores hidden files and directories (starting with a dot)
- Handles inaccessible files gracefully
"""

import sys
import pickle
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any

# Try to import tqdm for progress bar, fallback if not available
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None  # type: ignore


class FileIndex:
    """Efficient data structure for indexing files by (name, extension, size)."""

    def __init__(self):
        """Initialize the file index with a nested dictionary structure."""
        # Structure: {(filename, extension, size): [path1, path2, ...]}
        self.index: Dict[Tuple[str, str, int], List[str]] = defaultdict(list)

    def scan_directory(self, folder_path: str) -> None:
        """
        Recursively scan a directory and index all files.

        Args:
            folder_path: Root directory to scan
        """
        root = Path(folder_path)

        file_count = 0
        pbar: Optional[Any] = None
        if HAS_TQDM and tqdm is not None:
            pbar = tqdm(unit=" files", desc="Scanning", dynamic_ncols=True)

        def scan_recursive(path):
            """Recursively scan a directory with error handling."""
            nonlocal file_count, pbar
            try:
                for item in path.iterdir():
                    # Skip hidden files and directories
                    if item.name.startswith("."):
                        continue

                    try:
                        if item.is_file():
                            # Extract file info
                            filename = item.name
                            extension = item.suffix[1:] if item.suffix else ""
                            file_size = item.stat().st_size

                            # Create key and store full path
                            key = (filename, extension, file_size)
                            self.index[key].append(str(item.absolute()))
                            file_count += 1
                            if pbar is not None:
                                pbar.update(1)
                        elif item.is_dir():
                            scan_recursive(item)
                    except (OSError, PermissionError):
                        # Skip inaccessible files and directories
                        continue
            except (OSError, PermissionError):
                # Skip inaccessible directories
                pass

        scan_recursive(root)
        if pbar is not None:
            pbar.close()

    def get_locations(self, filename: str, extension: str = "", size: Optional[int] = None) -> List[str]:
        """
        Get all locations of files matching the given criteria.

        Args:
            filename: Name of the file (without extension)
            extension: File extension (without dot). Empty string for no extension
            size: File size in bytes. If None, returns all matches regardless of size

        Returns:
            List of full paths to matching files
        """
        matches = []
        for (fname, ext, fsize), paths in self.index.items():
            if fname == filename and ext == extension:
                if size is None or fsize == size:
                    matches.extend(paths)
        return matches

    def get_all_duplicates(self) -> Dict[Tuple[str, str, int], List[str]]:
        """
        Get all files that have duplicates (same name, extension, and size).

        Returns:
            Dictionary of only entries with multiple paths
        """
        return {k: v for k, v in self.index.items() if len(v) > 1}

    def get_file_info(self, filename: str, extension: str = "", size: Optional[int] = None) -> Dict:
        """
        Get detailed information about files matching the criteria.

        Args:
            filename: Name of the file
            extension: File extension
            size: File size in bytes

        Returns:
            Dictionary with count and paths
        """
        locations = self.get_locations(filename, extension, size)
        return {
            "filename": filename,
            "extension": extension,
            "size": size,
            "count": len(locations),
            "locations": locations
        }

    def print_summary(self, verbosity: int = 0) -> None:
        """Print a summary of the index.

        Args:
            verbosity: Verbosity level (0=summary only, 1=show duplicate names, 2=show names with paths)
        """
        total_files = sum(len(paths) for paths in self.index.values())
        unique_combos = len(self.index)
        duplicates = sum(1 for paths in self.index.values() if len(paths) > 1)

        print(f"\nFile Index Summary:")
        print(f"  Total files indexed: {total_files}")
        print(f"  Unique (filename, extension, size) combinations: {unique_combos}")
        print(f"  Combinations with duplicates: {duplicates}")

        if duplicates > 0 and verbosity > 0:
            print(f"\nDuplicate files (same name, extension, size):")
            for (fname, ext, size), paths in sorted(self.get_all_duplicates().items()):
                print(f"  {fname} ({size} bytes) - {len(paths)} copies")
                if verbosity >= 2:
                    for path in sorted(paths):
                        print(f"    {path}")

    def save_to_pickle(self, filepath: str) -> None:
        """
        Save the index dictionary to a pickle file.

        Args:
            filepath: Path to the output pickle file
        """
        with open(filepath, "wb") as f:
            pickle.dump(dict(self.index), f)
        print(f"\nIndex saved to: {filepath}")

    def load_from_pickle(self, filepath: str) -> None:
        """
        Load the index dictionary from a pickle file.

        Args:
            filepath: Path to the input pickle file
        """
        with open(filepath, "rb") as f:
            data = pickle.load(f)
            self.index = defaultdict(list, data)
        print(f"\nIndex loaded from: {filepath}")


def print_help():
    """Print help message."""
    help_text = """
Usage: python name_size_dup_step1.py <folder_path> [OPTIONS]

Description:
    Create an efficient data structure for mapping (filename, extension, size) to file paths.
    Recursively scans all files in a given directory and creates an index.
    A pickle file is always saved with the default name 'index.pkl' unless overridden.

Arguments:
    folder_path              Root directory to scan

Options:
    --save-pickle <file>     Save the index to a custom pickle filename (default: index.pkl)
    -v                       Verbosity level 1: Show duplicate file names
    -vv                      Verbosity level 2: Show duplicate file names with full paths
    --help                   Show this help message and exit

Examples:
    python name_size_dup_step1.py C:\\my\\folder
    python name_size_dup_step1.py C:\\my\\folder --save-pickle custom.pkl
    python name_size_dup_step1.py C:\\my\\folder -v
    python name_size_dup_step1.py C:\\my\\folder -vv --save-pickle custom.pkl
"""
    print(help_text)


def main():
    """Main function to demonstrate the FileIndex."""
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0 if "--help" in sys.argv or "-h" in sys.argv else 1)

    # Parse command line arguments
    folder_to_scan = None
    save_pickle_file = "index.pkl"  # Default pickle filename
    verbosity = 0  # 0=silent, 1=names, 2=with paths

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == "--save-pickle" and i + 1 < len(sys.argv):
            save_pickle_file = sys.argv[i + 1]
            i += 2
        elif arg == "-vv":
            verbosity = 2
            i += 1
        elif arg == "-v":
            verbosity = max(verbosity, 1)  # Don't downgrade if already -vv
            i += 1
        elif arg.startswith("--"):
            print(f"Error: Unknown option '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)
        elif not arg.startswith("-"):
            folder_to_scan = arg
            i += 1
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)

    # Validate required argument
    if folder_to_scan is None:
        print("Error: folder_path argument is required")
        print("Use --help for usage information")
        sys.exit(1)

    file_index = FileIndex()

    # Scan directory
    print(f"Scanning directory: {folder_to_scan}")
    file_index.scan_directory(folder_to_scan)

    # Print summary
    file_index.print_summary(verbosity=verbosity)

    # Always save to pickle with default or specified filename
    file_index.save_to_pickle(save_pickle_file)


if __name__ == "__main__":
    main()
