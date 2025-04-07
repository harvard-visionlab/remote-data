import os
import sys
import re
import logging

from urllib.parse import urlparse
from torch.hub import download_url_to_file
from typing import Mapping, Any, Optional, Dict
from pdb import set_trace

from visionlab.auth import sign_url_if_needed
from visionlab.remote_data.cache_dir import get_cache_root, get_cache_dir
from visionlab.remote_data.metadata import get_file_metadata
from visionlab.remote_data.decompress import decompress_if_needed

logger = logging.getLogger(__name__)

# matches bfd8deac from resnet18-bfd8deac.pth.tar
HASH_REGEX = re.compile(r'-([a-f0-9]*)\.(?:[^.]+(?:\.[^.]+)*)')

def download_from_url(url, cache_dir=None, progress=True, 
                      check_hash=False, hash_prefix=None, file_name=None,
                      expires_in_seconds=3600, s3_config=None,
                      use_hash_filename=False) -> Mapping[str, Any]:
    
    signed_url = sign_url_if_needed(url, s3_config=s3_config)

    if cache_dir is None: 
        if use_hash_filename:
            cache_dir = get_cache_root('hashid')
        else:
            cache_file_path = get_cache_dir(signed_url)
            cache_dir = os.path.dirname(cache_file_path)

    if use_hash_filename:
        metadata = get_file_metadata(signed_url)
        file_name = metadata['signature'] + metadata['ext']
        hash_prefix = metadata.get('sha256_prefix', hash_prefix)     
        
    cached_filename = torch_download_data_from_url(
        url = signed_url,
        data_dir = cache_dir,
        progress = progress,
        check_hash = check_hash,
        hash_prefix = hash_prefix,
        file_name = file_name,
    )

    logger.info(f"cached_filename: {cached_filename}")
    extracted_folder = decompress_if_needed(cached_filename)

    return cached_filename, extracted_folder
    
def torch_download_data_from_url(
    url: str,
    data_dir: Optional[str] = None,
    progress: bool = True,
    check_hash: bool = False,
    hash_prefix: Optional[str] = None,
    file_name: Optional[str] = None
) -> Dict[str, Any]:
    r"""Downloads the object at the given URL.

    If downloaded file is a .tar file or .tar.gz file, it will be automatically
    decompressed.

    If the object is already present in `data_dir`, it's deserialized and
    returned.

    The default value of ``data_dir`` is ``<hub_dir>/../data`` where
    ``hub_dir`` is the directory returned by :func:`~torch.hub.get_dir`.

    Args:
        url (str): URL of the object to download
        data_dir (str, optional): directory in which to save the object
        progress (bool, optional): whether or not to display a progress bar to stderr.
            Default: True
        check_hash(bool, optional): If True, the filename part of the URL should follow the naming convention
            ``filename-<sha256>.ext`` where ``<sha256>`` is the first eight or more
            digits of the SHA256 hash of the contents of the file. The hash is used to
            ensure unique names and to verify the contents of the file.
            Default: False
        file_name (str, optional): name for the downloaded file. Filename from ``url`` will be used if not set.

    Example:
        >>> state_dict = torch.hub.load_state_dict_from_url('https://s3.amazonaws.com/pytorch/models/resnet18-5c106cde.pth')

    """
    # Issue warning to move data if old env is set
    if os.getenv('TORCH_MODEL_ZOO'):
        warnings.warn('TORCH_MODEL_ZOO is deprecated, please use env TORCH_HOME instead')    
    
    if data_dir is None:
        data_dir = default_data_dir

    try:
        os.makedirs(data_dir, exist_ok=True)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # Directory already exists, ignore.
            pass
        else:
            # Unexpected OSError, re-raise.
            raise
        

    parts = urlparse(url)
    filename = os.path.basename(parts.path)
    if file_name is not None:
        filename = file_name
        
    if check_hash and hash_prefix is None:
        matches = HASH_REGEX.findall(filename) # matches is Optional[Match[str]]
        hash_prefix = matches[-1] if matches else None
        assert hash_prefix is not None, "check_hash is True, but the filename does not contain a hash_prefix. Expected <filename>-<hashid>.<ext>"
    
    if hash_prefix is not None:
        hash_dir = os.path.join(data_dir, hash_prefix)
        os.makedirs(hash_dir, exist_ok=True)
        cached_file = os.path.join(hash_dir, filename)
    else:
        cached_file = os.path.join(data_dir, filename)

    if not os.path.exists(cached_file):
        sys.stderr.write('Downloading: "{}" to {}\n'.format(url, cached_file))
        download_url_to_file(url, cached_file, hash_prefix, progress=progress)

    return cached_file