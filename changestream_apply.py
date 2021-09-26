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
Read data from kinesis and apply data to target MongoDB DB.
Required environment variables:
TARGET_USERNAME:DocumentDB Username
TARGET_PASSWORD:DOCUMENTDB PASSWORD
TARGET_CLUSTERENDPOINT: The URI of the MongoDB to apply data.
TARGET_SSL: whether connect MongoDB using SSL
SEQ_COLLECTION: The name of the collection in which to store kineis sequence state.
SEQ_DB: The name of the database in which to store sequence state.
TARGET_DB: The name of the database to apply for data.
MAX_LOOP: The max for the iterator loop.  
KINESIS_STREAM: The name of kinesis Data Stream   
"""

db_client = None                        # DocumentDB client - used as source                  
kinesis_client = None                   # Kinesis client - used as target                   
events_processed = 0                     # Track event processed

def get_db_client():
    """Return an authenticated connection to DocumentDB"""
    # Use a global variable so Lambda can reuse the persisted client on future invocations
    global db_client
    if db_client is None:
        print('Creating new DocumentDB client.')

        try:
            username = os.environ['TARGET_USERNAME']
            password = os.environ['TARGET_PASSWORD']
            clusterendpoint = os.environ['TARGET_CLUSTERENDPOINT']
            ssl = os.environ['TARGET_SSL']
            if ssl == 'False':
                db_client = MongoClient(clusterendpoint, ssl=False,retryWrites=False)
            else:
                db_client = MongoClient(clusterendpoint, ssl=True, ssl_ca_certs='rds-combined-ca-bundle.pem',retryWrites=False)
            # force the client to connect
            db_client.admin.command('ismaster')
            db_client["admin"].authenticate(name=username, password=password)
            print('Successfully created new DocumentDB client.') 
        except Exception as ex:
            logger.debug('Failed to create new DocumentDB client: {}'.format(ex))
            raise

    return db_client

def get_seq_collection_client():
    """Return a DocumentDB client for the collection in which we store processing state."""

    print('Creating state_collection_client.')
    try:
        db_client = get_db_client()
        seq_db_name = os.environ['SEQ_DB']
        seq_collection_name = os.environ['SEQ_COLLECTION']
        seq_collection = db_client[seq_db_name][seq_collection_name]
    except Exception as ex:
        print('Failed to create new seq collection client: {}'.format(ex))
        raise

    return seq_collection


def get_last_sequence():
    """Return the resume sequence corresponding to the last successfully processed change event."""
    try:
        last_sequence_id = None
        seq_collection = get_seq_collection_client()
        seq_doc = seq_collection.find_one({'currentState': True, 'stream': str(os.environ['KINESIS_STREAM'])})
        if seq_doc is not None:
            if 'lastSequence' in seq_doc: 
                last_sequence_id = seq_doc['lastSequence']
        else:
            seq_collection.insert_one({'stream': str(os.environ['KINESIS_STREAM']), 'currentState': True})
    except Exception as ex:
        print('Failed to return last processed id: {}'.format(ex))
        raise

    return last_sequence_id

def store_last_sequence(last_sequence_id):
    """Store the last sequence id corresponding to the last successfully processed change event."""

    print('Storing last processed id.')
    try:
        seq_collection = get_seq_collection_client()
        seq_collection.update_one({'stream': str(os.environ['KINESIS_STREAM'])},{'$set': {'lastSequence': last_sequence_id}})
    except Exception as ex:
        print('Failed to store last processed id: {}'.format(ex))
        raise

        
def apply_kinesis_event(message,db_client):
    """apply event from Kinesis"""
    try:
        if message['operation'] == 'insert':
            print('is insert')
            coll = message['coll']
            coll_client = db_client[coll]
            insert_body = json.loads(message['insert_body'])
            payload = {'_id':ObjectId(message['_id'])}
            payload.update(insert_body)
            print(payload)
            coll_client.insert_one(payload)
        if message['operation'] == 'delete':
            print('is delete')
            coll = message['coll']
            coll_client = db_client[coll]
            doc_id = message['_id']
            delete_body = {'_id':ObjectId(doc_id)}
            print(delete_body)
            coll_client.delete_one(delete_body)
        if message['operation'] == 'update':
           print('is update')
           coll = message['coll']    
           coll_client = db_client[coll]
           update_body = message['update_body']
           update_body = json.loads(message['update_body'])
           update_set = {"$set":update_body}
           print(update_set)
           id = {'_id':ObjectId(message['_id'])}
           coll_client.update_one(id,update_set)    
    except Exception as ex:
        print('Exception in apply message: {}'.format(ex))
        raise
def read_stream(stream_name,db_client):
    global events_processed
    """Read Data from Kinesis Data Stream"""
    kinesis_client = None 
    if kinesis_client is None:
        print('Creating new Kinesis client.')
        kinesis_client = boto3.client('kinesis') 
    try:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        last_sequence_id = get_last_sequence()
        if last_sequence_id is None:
            last_sequence_id = response['StreamDescription']['Shards'][0]['SequenceNumberRange']['StartingSequenceNumber']

        shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                      ShardId=shard_id,
                                                      ShardIteratorType='AFTER_SEQUENCE_NUMBER', StartingSequenceNumber=last_sequence_id)
        my_shard_iterator = shard_iterator['ShardIterator']
        record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=1)

        while 'NextShardIterator' in record_response:
            if len(record_response['Records']) == 0:
                print('no records captured, wait 1s and quit')
                time.sleep(1) 
                break
            message = json.loads(record_response['Records'][0]['Data'])
            apply_kinesis_event(message,db_client)
            events_processed += 1
            current_sequence = record_response['Records'][0]['SequenceNumber']
            store_last_sequence(current_sequence)
            record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=1)
    except Exception as ex:
        print('Exception in capturing message from Kinesis: {}'.format(ex))
        raise 

    
def main(args):
    """Read any new events from DocumentDB and apply them to an streaming/datastore endpoint."""
    global events_processed
    try:    
        target_db_client = None
        i=0
        stream_name = os.environ['KINESIS_STREAM']   
        # MongoDB Apply DB Connection set up
        db_client = get_db_client()
        target_db = os.environ['TARGET_DB']
        target_db_client = db_client[target_db]
        print('applying DB {}'.format(db_client))

        global kinesis_client
        if kinesis_client is None:
            print('Creating new Kinesis client.')
            kinesis_client = boto3.client('kinesis')

        # MongoDB sync set up
        last_sequence_id = get_last_sequence()
        print("last_sequence_id: {}".format(last_sequence_id))
        while i < int(os.environ['MAX_LOOP']):
              read_stream(stream_name,target_db_client)
              i += 1
              print(i)
    
    except Exception as ex:
        print('Exception in executing replication: {}'.format(ex))
        raise

    else:
        
        if events_processed > 0:

            print('Totally {} records to be processed'.format(events_processed))
            return{
                'statusCode': 200,
                'description': 'Success',
                'detail': json.dumps(str(events_processed)+ ' records processed successfully.')
            }
        else: 
            print("Totally no record is processed!")
            return{
                'statusCode': 201,
                'description': 'Success',
                'detail': json.dumps('No records to process.')
            }


if __name__ == '__main__':
    main(sys.argv[1:])

