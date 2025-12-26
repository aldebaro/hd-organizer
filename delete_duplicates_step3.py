"""
Delete duplicate files, keeping only the newest copy from each group.

This script reads the JSON output from get_duplicates.py and removes all
duplicate files except the most recent one in each group.

Safety features:
- Dry-run mode (default) to preview deletions without making changes
- Logging of all operations
- Confirmation prompts
- Rollback capability via logging
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


class DuplicateDeleter:
    """Delete duplicate files, keeping only the newest copy."""

    def __init__(self, json_file: str, dry_run: bool = True, log_file: str = None, verbose: bool = False):
        """
        Initialize the duplicate deleter.

        Args:
            json_file: Path to the duplicates JSON file from get_duplicates.py
            dry_run: If True, only preview deletions without actually deleting (default: True)
            log_file: Path to log file for tracking deletions. If None, creates one automatically
            verbose: If True, print all logging information to console (default: False)
        """
        self.json_file = json_file
        self.dry_run = dry_run
        self.verbose = verbose
        self.duplicates_data = self._load_json()

        # Set up logging
        if log_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"txt_duplicates_to_be_deleted{timestamp}.log"
        self.log_file = log_file
        self.log_lines = []

        self._log(f"DuplicateDeleter initialized")
        self._log(f"Dry-run mode: {dry_run}")
        self._log(f"JSON file: {json_file}")

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

    def preview_deletions(self) -> Tuple[int, int]:
        """
        Preview all files that would be deleted.

        Returns:
            Tuple of (total_files_to_delete, total_space_to_free_bytes)
        """
        self._log("\n" + "=" * 70)
        self._log("PREVIEW: Files that would be deleted")
        self._log("=" * 70)

        total_files = 0
        total_space = 0

        duplicates = self.duplicates_data.get("duplicates", [])

        for idx, group in enumerate(duplicates, 1):
            filename = group.get("filename", "")
            extension = group.get("extension", "")
            size_bytes = group.get("size_bytes", 0)
            newest_index = group.get("newest_index", 0)
            paths = group.get("paths", [])
            dates = group.get("dates", [])

            ext_display = f".{extension}" if extension else ""
            full_filename = f"{filename}{ext_display}"

            self._log(f"\nGroup {idx}: {full_filename} ({size_bytes} bytes)")
            self._log(f"  Keeping (newest): {paths[newest_index]}")
            if newest_index < len(dates):
                self._log(f"    Modified: {dates[newest_index]}")

            # List files to be deleted
            files_to_delete = []
            for i, path in enumerate(paths):
                if i != newest_index:
                    files_to_delete.append((path, dates[i] if i < len(dates) else ""))
                    total_files += 1
                    total_space += size_bytes

            if files_to_delete:
                self._log(f"  Deleting ({len(files_to_delete)} copies):")
                for path, date in files_to_delete:
                    self._log(f"    - {path}")
                    if date:
                        self._log(f"      Modified: {date}")

        self._log("\n" + "=" * 70)
        self._log(f"SUMMARY: {total_files} files would be deleted")
        self._log(
            f"Space to be freed: {total_space:,} bytes ({total_space / (1024**2):.2f} MB)")
        self._log("=" * 70)

        return total_files, total_space

    def delete_duplicates(self, confirm: bool = True) -> Tuple[int, int, List[str]]:
        """
        Delete all duplicate files, keeping only the newest copy.

        Args:
            confirm: If True, ask for confirmation before deleting

        Returns:
            Tuple of (deleted_count, space_freed_bytes, deleted_files_list)
        """
        if self.dry_run:
            self._log("\nDRY-RUN MODE: No files will be deleted")
            preview_files, preview_space = self.preview_deletions()
            return preview_files, preview_space, []

        # Ask for confirmation
        if confirm:
            print("\n" + "=" * 70)
            preview_files, preview_space = self.preview_deletions()
            print("=" * 70)
            response = input("\nProceed with deletion? (yes/no): ").strip().lower()
            if response != "yes":
                self._log("\nDeletion cancelled by user")
                return 0, 0, []

        self._log("\n" + "=" * 70)
        self._log("EXECUTING: Deleting duplicate files")
        self._log("=" * 70)

        deleted_count = 0
        space_freed = 0
        deleted_files = []
        failed_deletions = []

        duplicates = self.duplicates_data.get("duplicates", [])

        for idx, group in enumerate(duplicates, 1):
            filename = group.get("filename", "")
            extension = group.get("extension", "")
            newest_index = group.get("newest_index", 0)
            paths = group.get("paths", [])
            size_bytes = group.get("size_bytes", 0)

            ext_display = f".{extension}" if extension else ""
            full_filename = f"{filename}{ext_display}"

            self._log(f"\nProcessing group {idx}: {full_filename}")
            self._log(f"  Keeping: {paths[newest_index]}")

            # Delete all files except the newest
            for i, path in enumerate(paths):
                if i != newest_index:
                    try:
                        # Check if file exists
                        if not os.path.exists(path):
                            self._log(f"  SKIP (not found): {path}")
                            continue

                        # Delete the file
                        os.remove(path)
                        deleted_count += 1
                        space_freed += size_bytes
                        deleted_files.append(path)
                        self._log(f"  DELETED: {path}")

                    except PermissionError:
                        failed_deletions.append((path, "Permission denied"))
                        self._log(f"  ERROR (permission): {path}")
                    except OSError as e:
                        failed_deletions.append((path, str(e)))
                        self._log(f"  ERROR ({e}): {path}")

        # Summary
        self._log("\n" + "=" * 70)
        self._log("SUMMARY:")
        self._log(f"  Files deleted: {deleted_count}")
        self._log(
            f"  Space freed: {space_freed:,} bytes ({space_freed / (1024**2):.2f} MB)")

        if failed_deletions:
            self._log(f"  Failed deletions: {len(failed_deletions)}")
            for path, error in failed_deletions:
                self._log(f"    - {path}: {error}")

        self._log("=" * 70)

        return deleted_count, space_freed, deleted_files

    def analyze_storage_impact(self) -> None:
        """Analyze and display the storage impact of keeping only newest files."""
        self._log_and_print("\n" + "=" * 70)
        self._log_and_print("STORAGE IMPACT ANALYSIS")
        self._log_and_print("=" * 70)

        duplicates = self.duplicates_data.get("duplicates", [])

        total_duplicate_space = 0
        total_groups = 0
        total_files_with_duplicates = 0

        for group in duplicates:
            file_count = group.get("file_count", 0)
            size_bytes = group.get("size_bytes", 0)
            wasted_space = group.get("wasted_space_bytes", 0)

            total_groups += 1
            total_files_with_duplicates += file_count
            total_duplicate_space += wasted_space

        self._log_and_print(f"Total duplicate groups: {total_groups}")
        self._log_and_print(
            f"Total files involved in duplicates: {total_files_with_duplicates}")
        self._log_and_print(
            f"Total wasted space: {total_duplicate_space:,} bytes ({total_duplicate_space / (1024**2):.2f} MB)")
        self._log_and_print(
            f"Space to be freed by keeping newest: {total_duplicate_space:,} bytes ({total_duplicate_space / (1024**2):.2f} MB)")
        self._log_and_print("=" * 70)


def print_help():
    """Print help message."""
    help_text = """
Usage: python delete_duplicates_step3.py <json_file> [OPTIONS]

Description:
    Delete duplicate files, keeping only the newest copy from each group.
    Reads the JSON output from hash_byte_dup_step2.py and removes duplicates.

Arguments:
    json_file                Path to the duplicates JSON file from hash_byte_dup_step2.py

Options:
    --execute                Actually delete files (requires confirmation)
    --verbose                Print all logging information to console (default: quiet mode)
    --log-file <file>        Specify custom log file path
    --help                   Show this help message and exit

Examples:
    python delete_duplicates_step3.py duplicates_results_duplicates.json
    python delete_duplicates_step3.py duplicates_results_duplicates.json --execute --verbose
    python delete_duplicates_step3.py duplicates_results_duplicates.json --execute --log-file my_log.txt

Notes:
    - Default behavior is quiet mode with dry-run: previews deletions without printing details or making changes
    - Use --verbose flag to print all logging information to console
    - Use --execute flag to actually delete files
    - A confirmation prompt will appear before deletions proceed (with --execute)
    - All operations are logged to a file regardless of verbose setting
"""
    print(help_text)


def main():
    """Main function to delete duplicates."""
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0 if "--help" in sys.argv or "-h" in sys.argv else 1)

    json_file = sys.argv[1]
    execute = "--execute" in sys.argv
    verbose = "--verbose" in sys.argv
    log_file = None

    # Parse optional arguments
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == "--log-file" and i + 1 < len(sys.argv):
            log_file = sys.argv[i + 1]
            i += 2
        elif arg in ("--execute", "--verbose"):
            i += 1
        elif arg.startswith("--"):
            print(f"Error: Unknown option '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("Use --help for usage information")
            sys.exit(1)

    # Validate JSON file exists
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' not found")
        sys.exit(1)

    if verbose:
        print(f"Loading duplicates from: {json_file}\n")

    # Initialize deleter
    deleter = DuplicateDeleter(json_file, dry_run=not execute,
                               log_file=log_file, verbose=verbose)

    # Analyze storage impact
    deleter.analyze_storage_impact()

    # Preview or execute deletions
    if execute:
        if verbose:
            print("\nPreparing to execute deletions...\n")
        deleted_count, space_freed, deleted_files = deleter.delete_duplicates(
            confirm=True)
    else:
        if verbose:
            print("\nDRY-RUN MODE (preview only, no files will be deleted)\n")
        else:
            print("\nDRY-RUN MODE: No files will be deleted")
        deleted_count, space_freed, deleted_files = deleter.delete_duplicates(
            confirm=False)
        if verbose:
            print("\nTo actually delete files, use: --execute flag")
        else:
            print("Use --verbose --execute to see details and delete files")

    # Save log
    deleter.save_log()


if __name__ == "__main__":
    main()
