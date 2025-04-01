import os
import pty
import sys
import re
import subprocess
import logging
import threading

from pdb import set_trace

from visionlab.auth import normalize_uri
from .s5cmd_options import get_s5cmd_options_for_uri

logger = logging.getLogger(__name__) # Use module name for clarity

__all__ = ['s5cmd_download_file', 's5cmd_cp']

def s5cmd_download_file(remote_filepath: str, local_filepath: str,
                        profile: str=None, endpoint_url: str=None, region: str=None, 
                        dry_run: bool=False, show_progress: bool=True,
                        no_signed_option=None, endpoint_option=None):

    if os.path.isfile(local_filepath):
        return
        
    # get s5cmd_options needed for the command line call:
    s5cmd_options = get_s5cmd_options_for_uri(remote_filepath,
                                              profile=profile,
                                              endpoint_url=endpoint_url,
                                              region=region,
                                              no_signed_option=no_signed_option,
                                              endpoint_option=endpoint_option)
    
    s5cmd_options['dry_run_option'] = '--dry-run' if dry_run else None
    s5cmd_options['show_progress_option'] = '--show-progress' if show_progress else None
    remote_filepath = normalize_uri(remote_filepath)
    
    return s5cmd_cp(remote_filepath, local_filepath, s5cmd_options)

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
        
# def s5cmd_cp(src_filepath: str, dst_filepath: str, s5cmd_options=None) -> None:
#     """
#     Execute s5cmd cp command to download a file from S3 with real-time progress.
#     """
#     if os.path.isfile(dst_filepath):
#         return

#     s5cmd_options = s5cmd_options or {}
#     cmd_parts = [
#         "s5cmd",
#         s5cmd_options.get('dry_run_option'),
#         s5cmd_options.get('no_signed_option'),
#         s5cmd_options.get('endpoint_option'),
#         "cp",
#         s5cmd_options.get('show_progress_option'),
#         src_filepath,
#         dst_filepath
#     ]

#     cmd = " ".join(part for part in cmd_parts if part)
#     logger.info(cmd)

#     # Open a pseudo-terminal to force unbuffered output
#     master_fd, slave_fd = pty.openpty()
#     try:
#         proc = subprocess.Popen(
#             cmd,
#             shell=True,
#             stdout=slave_fd,
#             stderr=slave_fd,
#             env=s5cmd_options.get('env', os.environ.copy()),
#             text=True
#         )
#         os.close(slave_fd)  # We no longer need the slave fd in the parent process

#         # Read from the master fd and print the output in real time
#         while True:
#             try:
#                 output = os.read(master_fd, 1024)
#             except OSError:
#                 break  # Exit if there's an error reading
#             if not output:
#                 break
#             sys.stdout.write(output.decode())
#             sys.stdout.flush()
#     finally:
#         os.close(master_fd)
#         proc.wait()

#     return_code = proc.returncode
#     logger.info(f"s5cmd cp completed with return code: {return_code}")
#     if return_code != 0:
#         raise RuntimeError(f"s5cmd cp failed with return code {return_code}")
        
