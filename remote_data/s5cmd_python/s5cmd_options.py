import os
from pdb import set_trace

from visionlab.auth import (
    normalize_uri, 
    parse_uri, 
    check_public_s3_object,
    get_aws_credentials, 
    get_aws_credentials_with_provider_hint
)

__all__ = [
    'get_s5cmd_options_with_provider_hint',
    'get_s5cmd_options',
    'get_s5cmd_options_for_uri',
]

def _set_s5cmd_options_from_credentials(creds):
    env = None
    aws_access_key_id = creds.get('aws_access_key_id')
    aws_secret_access_key = creds.get('aws_secret_access_key')
    endpoint_url = creds.get('endpoint_url')
    region = creds.get('region')
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        env = {
            "AWS_ACCESS_KEY_ID": aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": aws_secret_access_key
        }
        if region:
            env['AWS_REGION'] = region
    return env, endpoint_url
    
def get_s5cmd_options_with_provider_hint(provider, profile=None, endpoint_url=None, region=None):
    creds = get_aws_credentials_with_provider_hint(provider, 
                                                   profile=profile, 
                                                   endpoint_url=endpoint_url, 
                                                   region=region)
    
    env, endpoint_url = _set_s5cmd_options_from_credentials(creds)
    return env, endpoint_url
    
def get_s5cmd_options(profile=None, endpoint_url=None, region=None):
    creds = get_aws_credentials(profile, endpoint_url=endpoint_url, region=region)
    env, endpoint_url = _set_s5cmd_options_from_credentials(creds)
    return env, endpoint_url

def get_s5cmd_options_for_uri(uri, profile=None, endpoint_url=None, region=None,
                              endpoint_option=None, no_signed_option=None, storage_options=None):
    """
        Sets env, endpoint_url, and no_signed_option for the given uri

        uri can be any s3-like uri
        s3://visionlab-datasets/
        aws://visionlab-datasets/
        wasabi://visionlab-datasets/
        machina://visionlab-datasets/
    """
    provider, bucket_name, object_key, _ = parse_uri(uri)
    s3_uri = normalize_uri(uri)    

    # copy current env
    env = os.environ.copy()

    # profile credentials can override env variables & endpoint_url
    if profile is not None:
        # use the provided profile
        aws_env,endpoint_url = get_s5cmd_options(profile=profile)            
    else:
        # no profile provided; check default profile names for this provider
        aws_env,endpoint_url = get_s5cmd_options_with_provider_hint(provider)

    # update env variables
    if aws_env:
        env.update(aws_env)
        
    # storage_options can override env variables & endpoint_url
    if storage_options:
        # endpoint_url and no_signed_option cannot be set via env variables
        # later we'll add them to the cmd manually:
        endpoint_url = storage_options.pop('endpoint_url', endpoint_url)
        no_signed_option = storage_options.pop('no_signed_option', no_signed_option)

        # AWS_ACCESS_KEY_ID and AWS_SECRET_KEY and AWS_REGION can be set as env
        env.update(storage_options)        

    # prepare the s5cmd command
    if no_signed_option or check_public_s3_object(s3_uri, endpoint_url=endpoint_url):
        no_signed_option = "--no-sign-request"

    if endpoint_url:
        endpoint_option = f"--endpoint-url {endpoint_url}"

    return dict(env=env, endpoint_option=endpoint_option, no_signed_option=no_signed_option)