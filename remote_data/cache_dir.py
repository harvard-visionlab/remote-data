import os
import warnings
from torch.hub import _get_torch_home
from enum import Enum
from pathlib import Path
from collections import OrderedDict
from litdata.constants import _IS_IN_STUDIO
from urllib.parse import urlparse
from pdb import set_trace

from visionlab.auth import (
    get_aws_credentials_with_provider_hint,
    normalize_uri,
    parse_uri,
    S3_PROVIDER_ENDPOINT_URLS
)
from .metadata import get_file_metadata


_DEFAULT_STUDIO_CACHEDIR = _get_torch_home().replace("/torch", "/datasets")
_DEFAULT_DIRS=OrderedDict([
    ('NETSCRATCH', "/n/netscratch/alvarez_lab/Lab/datasets/cache"),
    ('TIER1', "/n/alvarez_lab_tier1/Lab/datasets/cache"),
    ('DEVBOX', os.path.expanduser(path="~/work/DataLocal/cache")),
])

SHARED_DATASET_DIR = os.getenv('SHARED_DATASET_DIR')
if SHARED_DATASET_DIR is not None:
    _DEFAULT_DIRS['SHARED_DATASET_DIR'] = SHARED_DATASET_DIR

# Define the Platform Enum
class Platform(Enum):
    LIGHTNING_STUDIO = "lightning_studio"
    FAS_CLUSTER = "fas_cluster"
    DEVBOX = "devbox"
    
def is_slurm_available():
    return any(var in os.environ for var in ["SLURM_JOB_ID", "SLURM_CLUSTER_NAME"])

def check_platform():
    if _IS_IN_STUDIO:
        return Platform.LIGHTNING_STUDIO
    elif is_slurm_available():
        return Platform.FAS_CLUSTER
    else:
        return Platform.DEVBOX
    
def get_cache_root(*args):
    platform = check_platform()
    if platform == Platform.LIGHTNING_STUDIO:
        cache_root = os.getenv('STUDIO_CACHE', _DEFAULT_STUDIO_CACHEDIR)        
        Path(cache_root).mkdir(parents=True, exist_ok=True)
        for arg in args:
            cache_root = os.path.join(cache_root, arg)
        return cache_root
    else:
        for folder in _DEFAULT_DIRS.values():
            if os.path.exists(folder):
                cache_root = folder
                for arg in args:
                    cache_root = os.path.join(cache_root, arg)
                Path(cache_root).mkdir(parents=True, exist_ok=True)
                return cache_root
    warnings.warn("NO cache directory found!")
    return None

def get_cache_dir(source=None, cache_root=None, profile=None):
    '''
        Gets the cache directory for different sources, including s3, http(s), or mnt
    '''
    cache_root = get_cache_root() if cache_root is None else cache_root
    if source is None: return cache_root
    parsed = urlparse(source)
    scheme = parsed.scheme
    netloc = parsed.netloc
    key = parsed.path.lstrip("/")

    if Path(source).is_file():
        # looks like File on a mounted volume
        try:
            metadata = get_file_metadata(source)
            local_dir = os.path.join(cache_root, 'hashid', metadata['hash'])
        except:
            key = os.path.abspath(key).lstrip("/")
            local_dir = os.path.join(cache_root, 'mnt', netloc, key)        
    elif Path(source).parent.is_dir():
        # looks like a Directory on a mounted volume
        key = os.path.abspath(key).lstrip("/")
        local_dir = os.path.join(cache_root, 'mnt', netloc, key)
    elif scheme in ['https', 'http']:
        # treat as ordinary web url
        try:
            metadata = get_file_metadata(source)
            local_dir = os.path.join(cache_root, 'hashid', metadata['hash'])
        except:
            local_dir = os.path.join(cache_root, scheme, netloc, key)    
    elif scheme in S3_PROVIDER_ENDPOINT_URLS or parsed.netloc.startswith("s3"):
        provider = scheme
        s3_uri = normalize_uri(source)        
        endpoint = S3_PROVIDER_ENDPOINT_URLS[provider]
        endpoint = urlparse(endpoint).netloc
        local_dir = os.path.join(cache_root, "s3", provider, netloc, key)
    else:
        raise ValueError(f"Scheme `{scheme}` not supported")
    return local_dir