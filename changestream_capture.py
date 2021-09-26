#!/bin/env python

import json
import logging
import os
import string
import sys
import time
import boto3
import datetime
from bson import json_util
from pymongo import MongoClient
from pymongo.errors import OperationFailure                                    
import urllib.request                                                    

"""
Read data from a DocumentDB collection's change stream and replicate that data to Kinesis.
Required environment variables:
USERNAME:DocumentDB Username
PASSWORD:DOCUMENTDB PASSWORD
SSL: whether use SSL to connect DocumentDB
CLUSTERENDPOINT: The URI of the DocumentDB cluster to stream from.
STATE_COLLECTION: The name of the collection in which to store sync state.
STATE_DB: The name of the database in which to store sync state.
WATCHED_DB_NAME: The name of the database to watch for changes.
STATE_SYNC_COUNT: How many events to process before syncing state.
MAX_LOOP: The max for the iterator loop.     
KINESIS_STREAM : The Kinesis Stream name to publish DocumentDB events.

"""

db_client = None                        # DocumentDB client - used as source                  
kinesis_client = None                   # Kinesis client - used as target                                                         
# The error code returned when data for the requested resume token has been deleted
TOKEN_DATA_DELETED_CODE = 136

def get_db_client():
    """Return an authenticated connection to DocumentDB"""
    # Use a global variable so Lambda can reuse the persisted client on future invocations
    global db_client

    if db_client is None:
        print('Creating new DocumentDB client.')

        try:
            username = os.environ['USERNAME']
            password = os.environ['PASSWORD']
            clusterendpoint = os.environ['CLUSTERENDPOINT']
            ssl = os.environ['SSL']
            if ssl == 'False':
                db_client = MongoClient(clusterendpoint, ssl=False,retryWrites=False)
            else:
                db_client = MongoClient(clusterendpoint, ssl=True, ssl_ca_certs='rds-combined-ca-bundle.pem',retryWrites=False)
            # force the client to connect
            db_client.admin.command('ismaster')
            db_client["admin"].authenticate(name=username, password=password)
            print('Successfully created new DocumentDB client.') 
        except Exception as ex:
            print('Failed to create new DocumentDB client: {}'.format(ex))
            raise

    return db_client

def get_state_collection_client():
    """Return a DocumentDB client for the collection in which we store processing state."""

    print('Creating state_collection_client.')
    try:
        db_client = get_db_client()
        state_db_name = os.environ['STATE_DB']
        state_collection_name = os.environ['STATE_COLLECTION']
        state_collection = db_client[state_db_name][state_collection_name]
    except Exception as ex:
        print('Failed to create new state collection client: {}'.format(ex))
        raise

    return state_collection


def get_last_processed_id():
    """Return the resume token corresponding to the last successfully processed change event."""

    print('Returning last processed id.')
    try:
        last_processed_id = None
        state_collection = get_state_collection_client()
        state_doc = state_collection.find_one({'currentState': True, 'dbWatched': str(os.environ['WATCHED_DB_NAME'])})
        if state_doc is not None:
            if 'lastProcessed' in state_doc: 
                last_processed_id = state_doc['lastProcessed']
        else:
            state_collection.insert_one({'dbWatched': str(os.environ['WATCHED_DB_NAME']), 'currentState': True})
    except Exception as ex:
        print('Failed to return last processed id: {}'.format(ex))
        raise

    return last_processed_id

def store_last_processed_id(resume_token):
    """Store the resume token corresponding to the last successfully processed change event."""

    print('Storing last processed id.')
    try:
        state_collection = get_state_collection_client()
        state_collection.update_one({'dbWatched': str(os.environ['WATCHED_DB_NAME'])},{'$set': {'lastProcessed': resume_token}})
    except Exception as ex:
        print('Failed to store last processed id: {}'.format(ex))
        raise

        
def publish_kinesis_event(pkey,message):
    """send event to Kinesis"""
    # Use a global variable so Lambda can reuse the persisted client on future invocations
    global kinesis_client

    if kinesis_client is None:
        print('Creating new Kinesis client.')
        kinesis_client = boto3.client('kinesis')  

    try:
        # size of the messages  #######################################################
        print('Publishing message' + pkey + 'to Kinesis.')
        message_bytes = bytes(message, encoding='utf-8')
        print(message_bytes)
        response = kinesis_client.put_record(
            StreamName=os.environ['KINESIS_STREAM'],
            Data=message_bytes,
            PartitionKey=pkey
        )
    except Exception as ex:
        print('Exception in publishing message to Kinesis: {}'.format(ex))
        raise

def main(args):
    """Read any new events from DocumentDB and apply them to an streaming/datastore endpoint."""
    events_processed = 0
    try:       
        # DocumentDB watched collection set up
        db_client = get_db_client()
        watched_db = os.environ['WATCHED_DB_NAME']
        db_client = db_client[watched_db]
        print('Watching DB {}'.format(db_client))

        # DocumentDB sync set up
        state_sync_count = int(os.environ['STATE_SYNC_COUNT'])
        last_processed_id = get_last_processed_id()
        print("last_processed_id: {}".format(last_processed_id))


        with db_client.watch(full_document='updateLookup', resume_after=last_processed_id) as change_stream:
        #with db_client.watch(full_document='updateLookup') as change_stream:
            i = 0
            state = 0
            while change_stream.alive and i < int(os.environ['MAX_LOOP']):
                print(i)
            
                i += 1
                change_event = change_stream.try_next()
                print('Event: {}'.format(change_event))

                if change_event is None:
                    # On the first function invocation, we must sleep until the first event is processed,
                    # or processing will be trapped in a empty loop having never processed a first event
                    time.sleep(1)
                    continue
                else:
                    state +=1
                    print ('change_event is not null')
                    op_type = change_event['operationType']
                    print(op_type)
                    op_id = change_event['_id']['_data']

                    if op_type == 'insert':
                        doc_body = change_event['fullDocument']
                        doc_id = str(doc_body.pop("_id", None))
                        insert_body = change_event['fullDocument']
                        doc_coll = change_event['ns']['coll']
                        readable = datetime.datetime.fromtimestamp(change_event['clusterTime'].time).isoformat()
                        doc_body.update({'insert_body':json.dumps(insert_body),'operation':op_type,'coll':doc_coll,'timestamp':str(change_event['clusterTime'].time),'timestampReadable':str(readable)})
                        payload = {'_id':doc_id}
                        payload.update(doc_body)
                        # Publish event to Kinesis
                        if "KINESIS_STREAM" in os.environ:
                            publish_kinesis_event(str(op_id),json_util.dumps(payload))
                    if op_type == 'update':             
                        doc_body = change_event['fullDocument']
                        doc_id = str(doc_body.pop("_id", None))
                        update_body=doc_body
                        updatedescription=change_event['updateDescription']
                        doc_coll = change_event['ns']['coll']
                        readable = datetime.datetime.fromtimestamp(change_event['clusterTime'].time).isoformat()
                        doc_body.update({'update_body':json.dumps(update_body),'updatedescription':json.dumps(updatedescription),'operation':op_type,'coll':doc_coll,'timestamp':str(change_event['clusterTime'].time),'timestampReadable':str(readable)})
                        payload = {'_id':doc_id}
                        payload.update(doc_body)
                        # Publish event to Kinesis
                        if "KINESIS_STREAM" in os.environ:
                            publish_kinesis_event(str(op_id),json_util.dumps(payload))

                    if op_type == 'delete':
                        print(change_event)

                        doc_id = str(change_event['documentKey']['_id'])
                        readable = datetime.datetime.fromtimestamp(change_event['clusterTime'].time).isoformat()
                        doc_coll = change_event['ns']['coll']
                        payload = {'_id':doc_id,'coll':doc_coll,'operation':op_type,'timestamp':str(change_event['clusterTime'].time),'timestampReadable':str(readable)}
                        # Publish event to Kinesis
                        if "KINESIS_STREAM" in os.environ:
                            publish_kinesis_event(str(op_id),json_util.dumps(payload))

                        print('Processed event ID {}'.format(op_id))

                    events_processed += 1

                    if state >= state_sync_count:
                        # To reduce DocumentDB IO, only persist the stream state every N events
                        print('events processed wtf')
                        store_last_processed_id(change_stream.resume_token)
                        print('Synced token {} to state collection'.format(change_stream.resume_token))
                        state = 0
                        time.sleep(0.1)

    except OperationFailure as of:
        if of.code == TOKEN_DATA_DELETED_CODE:
            # Data for the last processed ID has been deleted in the change stream,
            # Store the last known good state so our next invocation
            # starts from the most recently available data
            store_last_processed_id(None)
        raise

    except Exception as ex:
        print('Exception in executing replication: {}'.format(ex))
        raise

    else:
        
        if events_processed > 0:

            store_last_processed_id(change_stream.resume_token)
            print('Synced token {} to state collection'.format(change_stream.resume_token))
            return{
                'statusCode': 200,
                'description': 'Success',
                'detail': json.dumps(str(events_processed)+ ' records processed successfully.')
            }
        else: 
            print('No records to process.')
            return{
                'statusCode': 201,
                'description': 'Success',
                'detail': json.dumps('No records to process.')
            }


if __name__ == '__main__':
    main(sys.argv[1:])
    
