import hashlib
import math
import os
import logging

from visionlab.auth import create_s3_client, parse_uri

logger = logging.getLogger(__name__) # Use module name for clarity

def _get_etag_from_s3(bucket_name, object_key, s3_client):
    """Gets the ETag from an S3 object."""
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"ETag: {response['ETag']}")
        return response['ETag'].strip('"')  # Remove surrounding quotes
    except Exception as e:
        logger.error(f"Error getting ETag: {e}")
        return None
        
def get_etag_from_s3_uri(s3_uri, s3_config=None):
    """Gets the Etag from an S3 object. Guesses credentials from s3_uri, 
    e.g., wasabi://visionlab-datasets/imagenetV2/class_info.json will trigger
    the use of wasabi credentials"""    
    provider, bucket_name, object_key, _ = parse_uri(s3_uri)
    s3_client = create_s3_client(s3_uri, s3_config=s3_config)

    return _get_etag_from_s3(bucket_name, object_key, s3_client)
     
def calculate_s3_etag(file_path, chunk_size=8*1024*1024):
    """
    Calculate the S3 ETag for a file, handling both single-part and multipart uploads.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of each chunk in bytes (default: 8MB, which is AWS's minimum)
    
    Returns:
        The calculated ETag string with quotes (like S3 returns)
    """
    # Get file size
    file_size = os.path.getsize(file_path)
    
    # For small files that would be uploaded in a single part
    if file_size <= chunk_size:
        # Simply return the MD5 hash for small files
        with open(file_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        return f'"{file_hash}"'
    
    # For larger files that would be multipart uploads
    md5s = []
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data).digest())
    
    # Calculate multipart ETag
    digests = b''.join(md5s)
    etag = hashlib.md5(digests).hexdigest()
    return f'"{etag}-{len(md5s)}"'