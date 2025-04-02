import os
import pty
import sys
import re
import subprocess
import logging
import threading

from typing import Optional, Dict, Any # Added Optional, Dict, Any
from filelock import FileLock, Timeout # Added FileLock, Timeout

from pdb import set_trace

from visionlab.auth import normalize_uri
from .s5cmd_options import get_s5cmd_options_for_uri

logger = logging.getLogger(__name__) # Use module name for clarity

__all__ = ['s5cmd_download_file', 's5cmd_cp']

# def s5cmd_download_file(remote_filepath: str, local_filepath: str,
#                         profile: str=None, endpoint_url: str=None, region: str=None, 
#                         dry_run: bool=False, show_progress: bool=True,
#                         no_signed_option=None, endpoint_option=None):

#     if os.path.isfile(local_filepath):
#         return
        
#     # get s5cmd_options needed for the command line call:
#     s5cmd_options = get_s5cmd_options_for_uri(remote_filepath,
#                                               profile=profile,
#                                               endpoint_url=endpoint_url,
#                                               region=region,
#                                               no_signed_option=no_signed_option,
#                                               endpoint_option=endpoint_option)
    
#     s5cmd_options['dry_run_option'] = '--dry-run' if dry_run else None
#     s5cmd_options['show_progress_option'] = '--show-progress' if show_progress else None
#     remote_filepath = normalize_uri(remote_filepath)
    
#     return s5cmd_cp(remote_filepath, local_filepath, s5cmd_options)

def s5cmd_download_file(remote_filepath: str, local_filepath: str,
                        s3_config=None,
                        dry_run: bool = False, show_progress: bool = True,
                        no_signed_option: Optional[bool] = None, # Changed to Optional[bool] for clarity
                        endpoint_option: Optional[str] = None, # Changed to Optional[str]
                        lock_timeout: int = 600) -> None: # Added lock_timeout parameter (e.g., 10 minutes)
    """
    Downloads a file using s5cmd, ensuring atomicity with a file lock.

    Args:
        remote_filepath: The source URI (e.g., s3://bucket/key, wasabi://bucket/key).
        local_filepath: The destination local file path.        
        s3_config:
            profile: AWS profile name (or equivalent).
            endpoint_url: Custom S3 endpoint URL.
            region: AWS region (or equivalent).
        dry_run: If True, simulate the command without actual transfer.
        show_progress: If True, display s5cmd progress.
        no_signed_option: If True, use --no-sign-request (for public buckets).
        endpoint_option: Explicit endpoint option string (overrides endpoint_url).
        lock_timeout: Maximum time in seconds to wait for the lock.
    """
    if s3_config is None:
        s3_config = {}
    profile = s3_config.get('profile')
    endpoint_url = s3_config.get('endpoint_url')
    region = s3_config.get('region')
    
    lock_filepath = local_filepath + ".lock"
    # Ensure the directory for the lock file exists
    os.makedirs(os.path.dirname(lock_filepath), exist_ok=True)
    lock = FileLock(lock_filepath, timeout=lock_timeout)

    try:
        logger.info(f"Acquiring lock for {local_filepath}...")
        with lock:
            logger.info(f"Lock acquired for {local_filepath}.")
            # Check if file exists *after* acquiring the lock
            if os.path.isfile(local_filepath) and os.path.getsize(local_filepath) > 0:
                logger.info(f"File {local_filepath} already exists and is non-empty. Skipping download.")
                return # Exit early if file exists

            logger.info(f"File {local_filepath} does not exist or is empty. Proceeding with download.")

            # Ensure the directory for the target file exists
            os.makedirs(os.path.dirname(local_filepath), exist_ok=True)

            # Get s5cmd_options needed for the command line call:
            s5cmd_options = get_s5cmd_options_for_uri(remote_filepath,
                                                      profile=profile,
                                                      endpoint_url=endpoint_url,
                                                      region=region,
                                                      no_signed_option=no_signed_option,
                                                      endpoint_option=endpoint_option)

            # Add command-specific options
            s5cmd_options['dry_run_option'] = '--dry-run' if dry_run else None
            # Note: s5cmd default is to show progress, explicitly disable if needed
            s5cmd_options['show_progress_option'] = '--show-progress' if show_progress else None

            # Normalize the remote path (e.g., ensure s3:// prefix)
            normalized_remote_filepath = normalize_uri(remote_filepath)

            # Call s5cmd_cp *within* the lock context
            s5cmd_cp(normalized_remote_filepath, local_filepath, s5cmd_options)

        logger.info(f"Lock released for {local_filepath}.")

    except Timeout:
        logger.error(f"Timeout occurred while waiting for lock on {lock_filepath}. Another process might be holding it.")
        raise # Re-raise the timeout error
    except Exception as e:
        logger.error(f"An error occurred during download for {local_filepath}: {e}")
         # Attempt to clean up potentially incomplete file on error
        if os.path.exists(local_filepath):
            try:
                # Check size again, maybe it finished despite error signal? Unlikely but possible.
                # Or maybe it's better to always remove on error? Depends on desired behavior.
                # For now, let's remove if it exists after an error during the s5cmd_cp call.
                if not (os.path.isfile(local_filepath) and os.path.getsize(local_filepath) > 0):
                     logger.warning(f"Removing potentially incomplete file: {local_filepath}")
                     os.remove(local_filepath)
            except OSError as rm_err:
                logger.error(f"Could not remove incomplete file {local_filepath}: {rm_err}")
        raise # Re-raise the original exception
    finally:
        # Clean up the lock file itself if it still exists
        # FileLock usually handles this, but manual cleanup can be a fallback
        if os.path.exists(lock_filepath):
            try:
                # Ensure the lock is released before trying to remove the file
                if lock.is_locked:
                     lock.release(force=True) # Force release if needed
                os.remove(lock_filepath)
                logger.debug(f"Removed lock file: {lock_filepath}")
            except OSError as e:
                # This might happen if another process cleaned it up first
                logger.warning(f"Could not remove lock file {lock_filepath} (might be gone already): {e}")
                
def s5cmd_cp(src_filepath: str, dst_filepath: str, s5cmd_options=None) -> None:
    """
    Execute s5cmd cp command to download a file from S3 with real-time progress, with less redundant errors.
    """
    if os.path.isfile(dst_filepath):
        return

    s5cmd_options = s5cmd_options or {}
    cmd_parts = [
        "s5cmd",
        s5cmd_options.get('dry_run_option'),
        s5cmd_options.get('no_signed_option'),
        s5cmd_options.get('endpoint_option'),
        "cp",
        s5cmd_options.get('show_progress_option'),
        src_filepath,
        dst_filepath
    ]

    cmd = " ".join(part for part in cmd_parts if part)
    logger.info(f"Executing command: {cmd}")

    master_fd, slave_fd = pty.openpty()
    pty_output = ""  # Capture pty output
    try:
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=slave_fd,
            stderr=slave_fd,
            env=s5cmd_options.get('env', os.environ.copy()),
            text=False  # Important: read as bytes
        )
        os.close(slave_fd)

        while True:
            try:
                output = os.read(master_fd, 1024)
            except OSError:
                break
            if not output:
                break
            decoded_output = output.decode('utf-8', errors='replace')
            pty_output += decoded_output
            sys.stdout.write(decoded_output)
            sys.stdout.flush()

        proc.wait()
        stdout, stderr = proc.communicate() #get any final errors

    finally:
        os.close(master_fd)

    return_code = proc.returncode
    logger.info(f"s5cmd cp completed with return code: {return_code}")

    if return_code != 0:
        error_message = f"s5cmd failed with return code {return_code}\n{cmd}"

        # Check if pty_output contains error-level information
        if "ERROR" not in pty_output and stderr:            
            error_message += f"\nstderr: {stderr.decode()}"

        raise RuntimeError(error_message)

