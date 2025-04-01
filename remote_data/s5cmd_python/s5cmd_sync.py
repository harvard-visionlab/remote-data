import os
import re
import subprocess
import logging
from tqdm.auto import tqdm

from pdb import set_trace

from visionlab.auth import normalize_uri
from .s5cmd_options import get_s5cmd_options_for_uri

logger = logging.getLogger(__name__) # Use module name for clarity

__all__ = ['s5cmd_download_file', 's5cmd_sync']

def s5cmd_download_file(remote_filepath: str, local_filepath: str,
                        profile: str=None, endpoint_url: str=None, region: str=None, 
                        dry_run: bool=False, no_signed_option=None, endpoint_option=None):

    # get s5cmd_options needed for the command line call:
    s5cmd_options = get_s5cmd_options_for_uri(remote_filepath,
                                              profile=profile,
                                              endpoint_url=endpoint_url,
                                              region=region,
                                              no_signed_option=no_signed_option,
                                              endpoint_option=endpoint_option)
    
    s5cmd_options['dry_run_option'] = '--dry-run' if dry_run else None
    remote_filepath = normalize_uri(remote_filepath)
    
    return s5cmd_sync(remote_filepath, local_filepath, s5cmd_options)
        
def s5cmd_sync(src_filepath: str, dst_filepath: str, s5cmd_options=None) -> None:
        """
        Execute s5cmd sync command to download a file from S3
        
        Args:
            src_filepath: S3-like URL of the file to download
            dst_filepath: Local path to download the file to
            s5cmd_options:
                dry_run_option
                no_signed_option
                endpoint_option
                env
        """
        # Create directory if it doesn't exist
        # os.makedirs(os.path.dirname(local_filepath), exist_ok=True)
        
        cmd_parts = ["s5cmd", 
                     s5cmd_options.get('dry_run_option'),
                     s5cmd_options.get('no_signed_option'), 
                     s5cmd_options.get('endpoint_option'),                    
                     "sync", 
                     src_filepath, 
                     dst_filepath]
    
        cmd = " ".join(part for part in cmd_parts if part)
        logger.info(cmd)

        # Execute the command
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=s5cmd_options.get('env', os.environ.copy()),
        )
        
        stdout, stderr = proc.communicate()
        stdout_text = stdout.decode().strip() if stdout else ""
        stderr_text = stderr.decode().strip() if stderr else ""
        return_code = proc.returncode

        logger.info(f"Output: {stdout_text}")
        logger.info(f"Error: {stderr_text}")
        logger.info(f"return_code: {return_code}")
    
        if stderr_text:
            error_message = (
                f"s5cmd sync operation failed with command `{cmd}`.\n"
                f"Return code: {return_code}\n"
                "This might be due to an incorrect file path, insufficient permissions, or network issues.\n"
            )
            
            if stdout_text:
                error_message += f"Output: {stdout_text}\n"
            error_message += f"Error: {stderr_text}\n"
                
            error_message += (
                "To resolve this issue, you might need to pass `storage_options` with the necessary credentials and endpoint."
                "- Example:\n"
                "  storage_options = {\n"
                '      "AWS_ACCESS_KEY_ID": "your-key",\n'
                '      "AWS_SECRET_ACCESS_KEY": "your-secret",\n'
                '      "S3_ENDPOINT_URL": "https://s3.example.com" (Optional if using AWS)\n'
                "  }\n"
            )
            
            # Verify if the file was actually created despite errors
            if not (os.path.exists(dst_filepath) and os.path.getsize(dst_filepath) > 0):
                raise RuntimeError(error_message)
            else:
                # Log the warning but proceed if file exists with content
                print(f"Warning: s5cmd reported errors but file was downloaded: {error_message}")
                