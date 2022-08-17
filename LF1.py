import json
from decimal import Decimal
import json
import urllib.request
import urllib.parse
import urllib.error
import boto3
from datetime import datetime
import requests

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})

    return response


def lambda_handler(event, context):
    '''Demonstrates S3 trigger that uses
    Rekognition APIs to detect faces, labels and index faces in S3 Object.
    '''
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    try:

        # Calls rekognition DetectLabels API to detect labels in S3 object
        response = detect_labels(bucket, key)

        
        # Use the S3 SDK’s headObject method to retrieve the S3 metadata
        custom_label_response = s3.head_object(Bucket=bucket, Key=key)
        
        print(custom_label_response)
        custom_labels =[] 
        if 'customlabels' in custom_label_response['Metadata']:
            custom_labels=custom_label_response['Metadata']['customlabels']
            
        custom_labels_array =[]
        if custom_labels:
            custom_labels_array=custom_labels.split(",")
        #print(custom_labels_array)
        
        #create json object
        json_object={}
        json_object['objectKey'] = key
        json_object['bucket'] = bucket
        json_object['createdTimestamp'] = datetime.utcnow().isoformat()
        json_object['labels']=[]
        
        for list in response['Labels']:
            json_object['labels'].append(list['Name'])
        
        if custom_labels_array:
            for label in custom_labels_array:
                json_object['labels'].append(label)
        
        print(json_object)
        #print(response['Labels'][0]['Name'])
        
        # upload json_object to ElasticSearch
        host ='https://search-photos-53ni23ydwcyes6ixxf6g2xrxu4.us-east-1.es.amazonaws.com/'
        region = 'us-east-1' 
        service = 'es'
        path = 'photos/_doc/'+json_object['objectKey']+'/'
        url = host + path
        r = requests.post(url, auth=("test1", "Test1test1!"), json=json_object)

        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
