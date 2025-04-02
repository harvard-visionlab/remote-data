import os
import sys
import re
import logging

from pathlib import Path
from typing import Mapping, Any, Optional, Dict
from pdb import set_trace

from visionlab.auth import check_is_s3_uri, normalize_uri, split_name
from visionlab.remote_data.s3_etag import get_etag_from_s3_uri, calculate_s3_etag
from visionlab.remote_data.s5cmd_python import s5cmd_download_file
from visionlab.remote_data.cache_dir import get_cache_root, get_cache_dir
from visionlab.remote_data.decompress import decompress_if_needed

# matches bfd8deac from resnet18-bfd8deac.pth.tar
HASH_REGEX = re.compile(r'-([a-f0-9]*)\.(?:[^.]+(?:\.[^.]+)*)')

logger = logging.getLogger(__name__)

def download_from_s3_uri(uri, cache_dir=None, progress=True, 
                         check_hash=False, hash_prefix=None, file_name=None,
                         s3_config=None, use_hash_filename=False) -> Mapping[str, Any]:

    logger.info(f"download_from_s3_uri: {uri}")
    
    # make sure this is an s3_uri
    is_s3_uri = check_is_s3_uri(uri)
    if is_s3_uri == False:
        raise ValueError(f"Expected an s3_uri, got {uri}")

    # get the file ETag (md5 hash-like)
    etag = get_etag_from_s3_uri(uri, s3_config=s3_config)
    logger.info(f"etag: {etag}")    
    
    # get the cache dir
    if cache_dir is None: 
        if use_hash_filename:
            cache_dir = get_cache_root('hashid')
        else:
            cache_file_path = get_cache_dir(uri)
            cache_dir = os.path.dirname(cache_file_path)
    Path(cache_dir).mkdir(parents=True, exist_ok=True)            
    logger.info(f"cache_dir: {cache_dir}")

    # normalize the uri
    s3_uri = normalize_uri(uri)
    logger.info(f"s3_uri: {s3_uri}")

    # Get the filename:
    if use_hash_filename:
        _, ext = split_name(s3_uri)
        file_name = etag + ext
    elif file_name is None:
        file_name = os.path.basename(s3_uri)
    logger.info(f"file_name: {cache_dir}")
    
    cached_filename = os.path.join(cache_dir, file_name)
    logger.info(f"cached_filename: {cached_filename}")

    # download the file if not present:
    if not os.path.isfile(cached_filename):
        
        s5cmd_download_file(
            remote_filepath=uri, 
            local_filepath=cached_filename,
            s3_config=s3_config,
            show_progress=progress
        )
        
        if check_hash:
            print("computing aws s3-style etag to check file integrity...")
            local_etag = calculate_s3_etag(cached_filename)
            target_etag = etag if hash_prefix is None else hash_prefix
            if local_etag != target_etag:
                msg = f'Remote File ETag {etag} does not match Local File ETag {local_etag}'
                logger.error(msg)
                raise ValueError(msg)

    # extract if this is a compressed file:
    extracted_folder = decompress_if_needed(cached_filename)
    
    return cached_filename, extracted_folder
    