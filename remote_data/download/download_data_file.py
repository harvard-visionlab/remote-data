
from visionlab.auth import check_is_s3_uri
from .download_from_s3_uri import download_from_s3_uri
from .download_from_url import download_from_url

def download_data_file(uri, cache_dir=None, progress=True,
                       check_hash=False, hash_prefix=None, file_name=None,
                       expires_in_seconds=3600, use_hash_filename=False,
                       s3_config=None):
    '''download remote data file
        Supports:
            - s3-compatible storage (public, or private - if the required 
                                     credentials are available or provided via s3_config)
            - http://, https:// urls
    '''
    
    # shared kwargs across fetch methods
    kwargs = dict(cache_dir=cache_dir,
                  progress=progress,
                  check_hash=check_hash,
                  hash_prefix=hash_prefix,
                  file_name=file_name,
                  use_hash_filename=use_hash_filename,
                  s3_config=s3_config)
    
    if check_is_s3_uri(uri):
        # use s5cmd for faster s3 downloads
        cached_file, extracted_dir = download_from_s3_uri(uri, **kwargs)
    else:
        # for all other files use url-downloading
        kwargs['expires_in_seconds'] = expires_in_seconds
        cached_file, extracted_dir = download_from_url(uri, **kwargs)

    return cached_file, extracted_dir 
                         