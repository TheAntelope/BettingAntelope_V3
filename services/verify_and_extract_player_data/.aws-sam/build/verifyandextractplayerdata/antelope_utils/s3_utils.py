import pickle
import boto3
import pandas as pd

def read_df_from_s3(bucket, key):
    """
    Pulls a file from s3 and returns it as a data frame
    Probably only works for .pkl, .csv, etc.
    
    Params:
    -------
    bucket: str
        s3 bucket name
    
    key: str
        s3 key to the file
        
    Returns:
    --------
    df: DataFrame
        
    """
    # connect ot s3 client
    s3client = boto3.client('s3')
    # get response
    response = s3client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    df = pickle.loads(body.read())
    df = df.sort_values(by=['Season','Week'])
    
    return df

def fetch_pkl_from_s3(bucket, s3_key):
    """
    takes a key and fetched the pickle file
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    body = response['Body']
    df = pickle.loads(body.read())
    
    return df

def fetch_pkl_from_s3_with_version(bucket, s3_key, version_id):
    """
    takes a key and fetched the pickle file
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=s3_key, VersionId=version_id)
    body = response['Body']
    df = pickle.loads(body.read())
    
    return df

def list_all_objects_version(bucket_name, prefix_name):
    session = boto3.session.Session()
    s3_client = session.client('s3')
    #try:
    result = s3_client.list_object_versions(Bucket=bucket_name, Prefix=prefix_name)
    return result
    #except:
        #print('Failed to list all object versions')