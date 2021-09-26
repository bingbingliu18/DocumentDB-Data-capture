import boto3
from bson import ObjectId
import json
from datetime import datetime
import time
import sys
import os
import pymongo
from pymongo import MongoClient
from pymongo.errors import OperationFailure

"""
recreate kinesis data stream
remove content from target DB state collection  (store sequence)
"""

source_db_client = None  
target_db_client = None                      # DocumentDB client - used as source                  
kinesis_client = None                   # Kinesis client - used as target                   
                    # Track event processed

def get_source_db_client():
    """Return an authenticated connection to DocumentDB"""
    # Use a global variable so Lambda can reuse the persisted client on future invocations
    global source_db_client

    if source_db_client is None:
        print('Creating new DocumentDB client.')

        try:
            username = os.environ['USERNAME']
            password = os.environ['PASSWORD']
            clusterendpoint = os.environ['CLUSTERENDPOINT']
            ssl = os.environ['SSL']
            if ssl == 'False':
                source_db_client = MongoClient(clusterendpoint, ssl=False,retryWrites=False)
            else:
                source_db_client = MongoClient(clusterendpoint, ssl=True, ssl_ca_certs='rds-combined-ca-bundle.pem',retryWrites=False)
            # force the client to connect
            source_db_client.admin.command('ismaster')
            source_db_client["admin"].authenticate(name=username, password=password)
            print('Successfully created new DocumentDB client.') 
        except Exception as ex:
            print('Failed to create new DocumentDB client: {}'.format(ex))
            raise

    return source_db_client

def get_state_collection_client():
    """Return a DocumentDB client for the collection in which we store processing state."""

    print('Creating state_collection_client.')
    try:
        db_client = get_source_db_client()
        state_db_name = os.environ['STATE_DB']
        state_collection_name = os.environ['STATE_COLLECTION']
        state_collection = db_client[state_db_name][state_collection_name]
    except Exception as ex:
        print('Failed to create new state collection client: {}'.format(ex))
        raise

    return state_collection

def get_target_db_client():
    """Return an authenticated connection to DocumentDB"""
    # Use a global variable so Lambda can reuse the persisted client on future invocations
    global target_db_client
    if target_db_client is None:
        print('Creating new DocumentDB client.')

        try:
            username = os.environ['TARGET_USERNAME']
            password = os.environ['TARGET_PASSWORD']
            clusterendpoint = os.environ['TARGET_CLUSTERENDPOINT']
            ssl = os.environ['TARGET_SSL']
            if ssl == 'False':
                target_db_client = MongoClient(clusterendpoint, ssl=False,retryWrites=False)
            else:
                target_db_client = MongoClient(clusterendpoint, ssl=True, ssl_ca_certs='rds-combined-ca-bundle.pem',retryWrites=False)
            # force the client to connect
            target_db_client.admin.command('ismaster')
            target_db_client["admin"].authenticate(name=username, password=password)
            print('Successfully created new DocumentDB client.') 
        except Exception as ex:
            logger.debug('Failed to create new DocumentDB client: {}'.format(ex))
            raise

    return target_db_client

def get_seq_collection_client():
    """Return a DocumentDB client for the collection in which we store processing state."""

    print('Creating state_collection_client.')
    try:
        db_client = get_target_db_client()
        seq_db_name = os.environ['SEQ_DB']
        seq_collection_name = os.environ['SEQ_COLLECTION']
        seq_collection = db_client[seq_db_name][seq_collection_name]
    except Exception as ex:
        print('Failed to create new seq collection client: {}'.format(ex))
        raise

    return seq_collection
   
def main(args):
    """reinitiate env"""
  
    try:    
        
        stream_name = os.environ['KINESIS_STREAM']   
        global kinesis_client
        if kinesis_client is None:
            print('Creating new Kinesis client.')
            kinesis_client = boto3.client('kinesis')
        #re-create Kinesis Data Stream
        print('Then recreate data stream:{}'.format(stream_name))
        create = kinesis_client.create_stream(StreamName=stream_name,ShardCount=1)

        while True:
            response = kinesis_client.describe_stream(StreamName=stream_name)
            if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
                break
            else:
                continue        
        # MongoDB Apply DB Connection set up
        seq_collection = get_seq_collection_client()
        print("drop stat sequence table")
        drop = seq_collection.drop()
        stat_collection = get_state_collection_client()
        print("drop stat changestream table")
        drop = stat_collection.drop()

    except Exception as ex:
        print('Exception in executing replication: {}'.format(ex))
        raise



if __name__ == '__main__':
    main(sys.argv[1:])

