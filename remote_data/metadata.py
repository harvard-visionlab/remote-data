import os
import re
import hashlib
import boto3
import requests
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
    check_public_s3_object
)

__all__ = ['get_file_metadata']

def get_file_metadata(source, read_limit=8192, hash_length=32, profile_name=None,
                      endpoint_url=None, region=None):
    """
    Retrieve file metadata, including size and unique identifier (content hash) based on the source.
    
    Parameters:
    source (str): The source URI, which can be an S3 URI, HTTP/HTTPS URL, or local file path.
    read_limit (int): The number of bytes to read for generating the hash.
    profile_name (str): AWS profile name to use for private S3 access if needed.
    region (str): AWS region for the S3 bucket.
    
    Returns:
    dict: A dictionary containing 'size' (in bytes) and 'hash' (SHA-256 hash of content sample).
    """    
    parsed = urlparse(source)
    hasher = hashlib.sha256()
    size = None

    if os.path.isfile(source):
        # For local files
        size = os.path.getsize(source)
        
        # Open the file and read a limited range of bytes to compute the hash
        with open(source, 'rb') as f:
            hasher.update(f.read(read_limit))
            
    elif parsed.scheme in ["http", "https"]:
        # For HTTP/HTTPS URLs
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
                                                           profile=profile_name,
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
    
    return {
        'scheme': parsed.scheme, 
        'netloc': parsed.netloc, 
        'path': parsed.path, 
        'size': size, 
        'hash': final_hash if size > 0 else None, 
        'read_limit': read_limit
    }