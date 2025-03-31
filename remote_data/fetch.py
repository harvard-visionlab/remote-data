from visionlab.auth import parse_uri

def fetch(remote_path, cache_dir=None, endpoint_url=None, region=None, 
          dryrun=False, profile_name=None): 
    
    parsed = parse_uri(remote_path)
    hasher = hashlib.sha256()

    if parsed.scheme == "s3":
        filepath = download_s3_file(remote_path, 
                                    cache_dir=cache_dir, 
                                    endpoint_url=endpoint_url,
                                    region=region, 
                                    dryrun=dryrun, 
                                    profile_name=profile_name)
    elif parsed.scheme in ["http", "https"] and parsed.netloc.startswith("s3"):
        endpoint_url, region = parse_s3_url(remote_path)
        filepath = download_s3_file(f"s3://{parsed.path[1:]}", 
                                    cache_dir=cache_dir, 
                                    endpoint_url=endpoint_url,
                                    region=region, 
                                    dryrun=dryrun, 
                                    profile_name=profile_name)
    elif parsed.scheme in ["http", "https"]:
        filepath = download_from_url(remote_path)

    return filepath