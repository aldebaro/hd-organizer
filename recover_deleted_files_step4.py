"""
Recover deleted duplicate files by copying the newest file back to all paths.

This script reads the JSON output from hash_byte_dup_step2.py and recovers
deleted duplicate files by copying the newest (retained) file back to all
other paths in each duplicate group that match a search string.

Safety features:
- Dry-run mode (default) to preview recoveries without making changes
- Logging of all operations
- Confirmation prompts
- Only recovers files where a search string matches the path
- Progress bar showing recovery status
"""

import sys
import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from tqdm import tqdm


class DuplicateRecoverer:
    """Recover deleted duplicate files by copying the newest copy back."""

    def __init__(self, json_file: str, search_string: str = None, dry_run: bool = True, log_file: str = None, verbose: bool = False, recover_all: bool = False):
        """
        Initialize the duplicate recoverer.

        Args:
            json_file: Path to the duplicates JSON file from hash_byte_dup_step2.py
            search_string: String to search for in file paths (case-insensitive). Ignored if recover_all is True.
            dry_run: If True, only preview recoveries without making changes (default: True)
            log_file: Path to log file for tracking recoveries. If None, creates one automatically
            verbose: If True, print all logging information to console (default: False)
            recover_all: If True, recover all duplicate files without filtering (default: False)
        """
        self.json_file = json_file
        self.search_string = search_string.lower() if search_string else None
        self.dry_run = dry_run
        self.verbose = verbose
        self.recover_all = recover_all
        self.duplicates_data = self._load_json()

        # Set up logging
        if log_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if recover_all:
                log_file = f"txt_recovered_files_all_{timestamp}.log"
            else:
                log_file = f"txt_recovered_files_{search_string}_{timestamp}.log"
        self.log_file = log_file
        self.log_lines = []

        self._log(f"DuplicateRecoverer initialized")
        self._log(f"Dry-run mode: {dry_run}")
        self._log(f"JSON file: {json_file}")
        if recover_all:
            self._log(f"Mode: Recover all files")
        else:
            self._log(f"Search string: {search_string}")

    def _load_json(self) -> Dict:
        """Load the duplicates JSON file."""
        try:
            with open(self.json_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: JSON file '{self.json_file}' not found")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in '{self.json_file}'")
            sys.exit(1)

    def _log(self, message: str) -> None:
        """Add a message to the log. Print to console only if verbose mode is enabled."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log_lines.append(log_message)
        if self.verbose:
            print(log_message)

    def _log_and_print(self, message: str) -> None:
        """Add a message to the log and always print to console regardless of verbose setting."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log_lines.append(log_message)
        print(log_message)

    def save_log(self) -> None:
        """Save the log to file."""
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write("\n".join(self.log_lines))
        print(f"\nLog saved to: {self.log_file}")

    def _filter_matching_groups(self) -> List[Dict]:
        """
        Filter duplicate groups. If recover_all is True, return all groups.
        Otherwise, return groups where at least one path contains the search string.

        Returns:
            List of duplicate groups that match the criteria
        """
        duplicates = self.duplicates_data.get("duplicates", [])

        if self.recover_all:
            return duplicates

        matching_groups = []
        for group in duplicates:
            paths = group.get("paths", [])
            # Check if any path contains the search string
            if any(self.search_string in path.lower() for path in paths):
                matching_groups.append(group)

        return matching_groups

    def preview_recoveries(self) -> Tuple[int, int]:
        """
        Preview all files that would be recovered.

        Returns:
            Tuple of (total_files_to_recover, total_space_to_restore_bytes)
        """
        self._log("\n" + "=" * 70)
        self._log("PREVIEW: Files that would be recovered")
        self._log("=" * 70)

        matching_groups = self._filter_matching_groups()

        if not matching_groups:
            if self.recover_all:
                self._log(f"\nNo duplicate groups found")
            else:
                self._log(
                    f"\nNo duplicate groups match search string: '{self.search_string}'")
            self._log("=" * 70)
            return 0, 0

        total_files = 0
        total_space = 0

        for idx, group in enumerate(matching_groups, 1):
            filename = group.get("filename", "")
            extension = group.get("extension", "")
            size_bytes = group.get("size_bytes", 0)
            newest_index = group.get("newest_index", 0)
            paths = group.get("paths", [])
            dates = group.get("dates", [])

            ext_display = f".{extension}" if extension else ""
            full_filename = f"{filename}{ext_display}"

            self._log(f"\nGroup {idx}: {full_filename} ({size_bytes} bytes)")
            self._log(f"  Source (newest): {paths[newest_index]}")
            if newest_index < len(dates):
                self._log(f"    Modified: {dates[newest_index]}")

            # List files to be recovered
            files_to_recover = []
            for i, path in enumerate(paths):
                if i != newest_index:
                    # Check if this path should be recovered
                    if self.recover_all or self.search_string in path.lower():
                        files_to_recover.append(
                            (path, dates[i] if i < len(dates) else ""))
                        total_files += 1
                        total_space += size_bytes

            if files_to_recover:
                self._log(f"  Recovering ({len(files_to_recover)} copies):")
                for path, date in files_to_recover:
                    self._log(f"    - {path}")
                    if date:
                        self._log(f"      Original date: {date}")

        self._log("\n" + "=" * 70)
        self._log(f"SUMMARY: {total_files} files would be recovered")
        self._log(
            f"Total space to restore: {total_space:,} bytes ({total_space / (1024**2):.2f} MB)")
        self._log("=" * 70)

        return total_files, total_space

    def recover_duplicates(self, confirm: bool = True) -> Tuple[int, int, List[str]]:
        """
        Recover duplicate files by copying the newest copy to all other paths.

        Args:
            confirm: If True, ask for confirmation before recovering

        Returns:
            Tuple of (recovered_count, space_restored_bytes, recovered_files_list)
        """
        if self.dry_run:
            self._log("\nDRY-RUN MODE: No files will be recovered")
            preview_files, preview_space = self.preview_recoveries()
            return preview_files, preview_space, []

        # Ask for confirmation
        if confirm:
            print("\n" + "=" * 70)
            preview_files, preview_space = self.preview_recoveries()
            print("=" * 70)
            response = input("\nProceed with recovery? (yes/no): ").strip().lower()
            if response != "yes":
                self._log("\nRecovery cancelled by user")
                return 0, 0, []

        self._log("\n" + "=" * 70)
        self._log("EXECUTING: Recovering duplicate files")
        self._log("=" * 70)

        recovered_count = 0
        space_restored = 0
        recovered_files = []
        failed_recoveries = []

        matching_groups = self._filter_matching_groups()

        # Count total files to recover for progress bar
        total_files_to_recover = 0
        for group in matching_groups:
            paths = group.get("paths", [])
            newest_index = group.get("newest_index", 0)
            for i, path in enumerate(paths):
                if i != newest_index and (self.recover_all or self.search_string in path.lower()):
                    total_files_to_recover += 1

        # Create progress bar (disabled in verbose mode to avoid clutter)
        progress_bar = tqdm(total=total_files_to_recover, desc="Recovering files", unit="file",
                            disable=self.verbose)

        for idx, group in enumerate(matching_groups, 1):
            filename = group.get("filename", "")
            extension = group.get("extension", "")
            newest_index = group.get("newest_index", 0)
            paths = group.get("paths", [])
            size_bytes = group.get("size_bytes", 0)

            ext_display = f".{extension}" if extension else ""
            full_filename = f"{filename}{ext_display}"

            source_path = paths[newest_index]

            # Check if source file exists
            if not os.path.exists(source_path):
                self._log(f"\nGroup {idx}: {full_filename}")
                self._log(f"  ERROR: Source file not found: {source_path}")
                continue

            self._log(f"\nGroup {idx}: {full_filename}")
            self._log(f"  Source: {source_path}")

            # Recover all files except the newest (that match search string or if recover_all)
            for i, path in enumerate(paths):
                if i != newest_index and (self.recover_all or self.search_string in path.lower()):
                    try:
                        # Create parent directory if it doesn't exist
                        parent_dir = os.path.dirname(path)
                        if parent_dir and not os.path.exists(parent_dir):
                            os.makedirs(parent_dir, exist_ok=True)
                            self._log(f"  MKDIR: {parent_dir}")

                        # Copy file
                        shutil.copy2(source_path, path)
                        recovered_count += 1
                        space_restored += size_bytes
                        recovered_files.append(path)
                        self._log(f"  RECOVERED: {path}")

                    except PermissionError:
                        failed_recoveries.append((path, "Permission denied"))
                        self._log(f"  ERROR (permission): {path}")
                    except OSError as e:
                        failed_recoveries.append((path, str(e)))
                        self._log(f"  ERROR ({e}): {path}")
                    finally:
                        progress_bar.update(1)

        progress_bar.close()

        # Summary
        self._log("\n" + "=" * 70)
        self._log("SUMMARY:")
        self._log(f"  Files recovered: {recovered_count}")
        self._log(
            f"  Space restored: {space_restored:,} bytes ({space_restored / (1024**2):.2f} MB)")

        if failed_recoveries:
            self._log(f"  Failed recoveries: {len(failed_recoveries)}")
            for path, error in failed_recoveries:
                self._log(f"    - {path}: {error}")

        self._log("=" * 70)

        return recovered_count, space_restored, recovered_files

    def analyze_recovery_impact(self) -> None:
        """Analyze and display the impact of recovery for matching files."""
        self._log_and_print("\n" + "=" * 70)
        self._log_and_print("RECOVERY IMPACT ANALYSIS")
        self._log_and_print("=" * 70)

        matching_groups = self._filter_matching_groups()

        if not matching_groups:
            if self.recover_all:
                self._log_and_print(f"No duplicate groups found")
            else:
                self._log_and_print(
                    f"No duplicate groups match search string: '{self.search_string}'")
            self._log_and_print("=" * 70)
            return

        total_matching_groups = len(matching_groups)
        total_matching_files = 0
        total_space_to_restore = 0

        for group in matching_groups:
            paths = group.get("paths", [])
            newest_index = group.get("newest_index", 0)
            size_bytes = group.get("size_bytes", 0)

            # Count files to recover (all except newest, or filtered by search string)
            if self.recover_all:
                matching_count = len(paths) - 1  # All except newest
            else:
                matching_count = sum(
                    1 for i, path in enumerate(paths)
                    if i != newest_index and self.search_string in path.lower()
                )

            total_matching_files += matching_count
            total_space_to_restore += matching_count * size_bytes

        if self.recover_all:
            self._log_and_print(f"Mode: Recover all files")
        else:
            self._log_and_print(f"Search string: '{self.search_string}'")
        self._log_and_print(f"Matching duplicate groups: {total_matching_groups}")
        self._log_and_print(f"Total files to recover: {total_matching_files}")
        self._log_and_print(
            f"Total space to restore: {total_space_to_restore:,} bytes ({total_space_to_restore / (1024**2):.2f} MB)")
        self._log_and_print("=" * 70)


def print_help():
    """Print help message."""
    help_text = """
Usage: python recover_deleted_files_step4.py <json_file> [search_string] [OPTIONS]

Description:
    Recover deleted duplicate files by copying the newest file back to all paths
    that match a search string. Reads the JSON output from hash_byte_dup_step2.py.

Arguments:
    json_file                Path to the duplicates JSON file from hash_byte_dup_step2.py
    search_string            String to search for in file paths (case-insensitive). Not required if using --recover-all.

Options:
    --recover-all            Recover all duplicate files without filtering (ignores search_string)
    --execute                Actually recover files (requires confirmation)
    --verbose                Print all logging information to console (default: quiet mode)
    --log-file <file>        Specify custom log file path
    --help                   Show this help message and exit

Examples:
    python recover_deleted_files_step4.py duplicates_results_duplicates.json python
    python recover_deleted_files_step4.py duplicates_results_duplicates.json python --execute --verbose
    python recover_deleted_files_step4.py duplicates_results_duplicates.json "Documents" --execute
    python recover_deleted_files_step4.py duplicates_results_duplicates.json --recover-all
    python recover_deleted_files_step4.py duplicates_results_duplicates.json --recover-all --execute --verbose

Notes:
    - Default behavior is quiet mode with dry-run: previews recoveries without making changes
    - Search string is case-insensitive
    - Only paths containing the search string will be recovered (unless --recover-all is used)
    - Use --recover-all to recover all duplicate files
    - Use --verbose flag to print all logging information to console
    - Use --execute flag to actually recover files
    - A confirmation prompt will appear before recovery proceeds (with --execute)
    - All operations are logged to a file regardless of verbose setting
    - Source file (newest copy) must exist for recovery to work
"""
    print(help_text)


def main():
    """Main function to recover duplicates."""
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0 if "--help" in sys.argv or "-h" in sys.argv else 1)

    json_file = sys.argv[1]
    recover_all = "--recover-all" in sys.argv
    execute = "--execute" in sys.argv
    verbose = "--verbose" in sys.argv
    log_file = None
    search_string = None

    # Parse positional arguments and options
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == "--log-file" and i + 1 < len(sys.argv):
            log_file = sys.argv[i + 1]
            i += 2
        elif arg in ("--execute", "--verbose", "--recover-all"):
            i += 1
        elif arg.startswith("--"):
            print(f"Error: Unknown option '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)
        elif not recover_all and search_string is None:
            # This is the search string argument
            search_string = arg
            i += 1
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)

    # Validate arguments
    if not recover_all and search_string is None:
        print("Error: search_string is required unless using --recover-all")
        print("Use --help for usage information")
        sys.exit(1)

    # Validate JSON file exists
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' not found")
        sys.exit(1)

    if verbose:
        print(f"Loading duplicates from: {json_file}")
        if recover_all:
            print(f"Mode: Recover all files\n")
        else:
            print(f"Search string: {search_string}\n")

    # Initialize recoverer
    recoverer = DuplicateRecoverer(json_file, search_string, dry_run=not execute,
                                   log_file=log_file, verbose=verbose, recover_all=recover_all)

    # Analyze recovery impact
    recoverer.analyze_recovery_impact()

    # Preview or execute recoveries
    if execute:
        if verbose:
            print("\nPreparing to execute recovery...\n")
        recovered_count, space_restored, recovered_files = recoverer.recover_duplicates(
            confirm=True)
    else:
        if verbose:
            print("\nDRY-RUN MODE (preview only, no files will be recovered)\n")
        else:
            print("\nDRY-RUN MODE: No files will be recovered")
        recovered_count, space_restored, recovered_files = recoverer.recover_duplicates(
            confirm=False)
        if verbose:
            print("\nTo actually recover files, use: --execute flag")
        else:
            print("Use --verbose --execute to see details and recover files")

    # Save log
    recoverer.save_log()


if __name__ == "__main__":
    main()
