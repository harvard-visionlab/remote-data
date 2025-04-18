import os
import re
import hashlib
import boto3
import requests
from pathlib import Path
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.client import Config
from pdb import set_trace

from visionlab.auth import (
    get_aws_credentials_with_provider_hint, 
    parse_uri,
    normalize_uri, 
    S3_PROVIDER_ENDPOINT_URLS, 
    check_public_s3_object,
    sign_url_if_needed
)

__all__ = ['get_file_metadata']

# matches bfd8deac from resnet18-bfd8deac.pth.tar
HASH_REGEX = re.compile(r'-([a-f0-9]*)\.(?:[^.]+(?:\.[^.]+)*)')

def split_name(path: Path):
    """Split a path into the stem and the complete extension (all suffixes)."""
    path = Path(path)
    suffixes = path.suffixes
    if suffixes:
        ext = "".join(suffixes)
        stem = path.name[:-len(ext)]
    else:
        ext = ""
        stem = path.name
    return stem, ext
    
def get_file_metadata(source, read_limit=8192*8, hash_length=32, s3_config=None):
    """
    Retrieve file metadata, including size and unique identifier (content hash) based on the source.
    
    Parameters:
    source (str): The source URI, which can be an S3 URI, HTTP/HTTPS URL, or local file path.
    read_limit (int): The number of bytes to read for generating the hash (64KB default)
    profile_name (str): AWS profile name to use for private S3 access if needed.
    region (str): AWS region for the S3 bucket.
    
    Returns:
    dict: A dictionary containing 'size' (in bytes) and 'hash' (SHA-256 hash of content sample).
    """    
    if s3_config is None:
        s3_config = {}
    profile = s3_config.get('profile')
    endpoint_url = s3_config.get('endpoint_url')
    region = s3_config.get('region')
    
    parsed = urlparse(source)
    hasher = hashlib.sha256()
    size = None

    if os.path.isfile(os.path.expanduser(source)):
        source = os.path.expanduser(source)
        # For local files
        size = os.path.getsize(source)
        
        # Open the file and read a limited range of bytes to compute the hash
        with open(source, 'rb') as f:
            hasher.update(f.read(read_limit))
            
    elif parsed.scheme in ["http", "https"]:
        # For HTTP/HTTPS URLs
        source = sign_url_if_needed(source, s3_config=s3_config)
        head_response = requests.head(source)
        
        size = head_response.headers.get('Content-Length')

        # If Content-Length is not provided, perform a full GET request to get the file size
        if size is None:
            # Fetch a limited range of bytes to compute the hash
            range_response = requests.get(source, headers={'Range': f'bytes=0-{read_limit-1}'})
            hasher.update(range_response.content)
            
            # Make a second request without range to get the full size
            full_response = requests.get(source, stream=True)
            size = int(full_response.headers.get('Content-Length', 0))
            full_response.close()
        else:
            # If Content-Length is available, convert it to an integer
            size = int(size)
            
            # Fetch limited data for the hash
            range_response = requests.get(source, headers={'Range': f'bytes=0-{read_limit-1}'})
            hasher.update(range_response.content)
            
    elif parsed.scheme in S3_PROVIDER_ENDPOINT_URLS:
        # Assuming an S3_path (aws, or aws compatible)
        s3_uri = normalize_uri(source)
        provider, bucket_name, key, endpoint_hint = parse_uri(source)

        # Check if the S3 object is public
        is_public = check_public_s3_object(source)

        if not is_public:   
            creds = get_aws_credentials_with_provider_hint(provider,
                                                           profile=profile,
                                                           endpoint_url=endpoint_url,
                                                           region=region)
            if endpoint_url is None:
                endpoint_url = creds.get("endpoint_url")
            if region is None:
                region = creds.get("region")
            print(endpoint_url, region, bucket_name, key)
            # Initialize the S3 client with credentials for private access
            s3 = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=creds["aws_access_key_id"],
                aws_secret_access_key=creds["aws_secret_access_key"],
                aws_session_token=creds.get("aws_session_token"),
                endpoint_url=creds.get("endpoint_url")
            )
        else:
            # Public access: Initialize the S3 client without credentials
            s3 = boto3.client('s3', 
                              region_name=region,
                              endpoint_url=endpoint_url,
                              config=Config(signature_version=UNSIGNED))

        # Get object metadata and partial content
        try:
            response = s3.head_object(Bucket=bucket_name, Key=key)
            size = response['ContentLength']
            # Fetch a limited range of bytes to compute a consistent hash
            range_response = s3.get_object(Bucket=bucket_name, Key=key, Range=f'bytes=0-{read_limit-1}')
            hasher.update(range_response['Body'].read())
        except ClientError as e:
            print(f"Could not access object {key} in bucket {bucket_name}: {e}")
            return
    else:
        raise ValueError("Unsupported source type. Must be S3 URI, URL, or local file path.")

    unique_id = hasher.hexdigest()
    final_hash = unique_id[:hash_length] if hash_length else unique_id
    if size > 0:
        hash_id = final_hash
        signature = f"{hash_id}-{size}"
    else:
        hash_id = None
        signature = f"{parsed.netloc}{parsed.path}"

    # get the filename
    filename = Path(source).name
    stem, ext = split_name(source)

    # check filename for a hash_prefix
    matches = HASH_REGEX.findall(filename) # matches is Optional[Match[str]]
    sha256_prefix = matches[-1] if matches else None

    return {
        'scheme': parsed.scheme, 
        'netloc': parsed.netloc, 
        'path': parsed.path, 
        'size': size, 
        'partial_hash': hash_id, 
        'sha256_prefix': sha256_prefix,
        'read_limit': read_limit,
        'signature': signature,
        'filename': filename,
        'stem': stem,
        'ext': ext
    }