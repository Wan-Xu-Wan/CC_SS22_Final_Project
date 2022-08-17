import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
import urllib.request 
from PIL import Image 



region = 'us-east-1'
service = 'es'
#credentials = boto3.Session().get_credentials()
#awsauth = AWS4Auth(ACCESS_ID, ACCESS_KEY, region, service)
client = boto3.client('lex-runtime')


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    return response

def getSize(url):
    image = Image.open(urllib.request.urlopen(url)) 
    return image.size 

def lambda_handler(event, context):
    # q = event['queryStringParameters']['q']
    q = event['q']
    print('here is:',q)
    if q == '':
        response = {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin":"*","Content-Type":"application/json"},
            "body": [],
            "isBase64Encoded": False}
        return response
    client = boto3.client('lex-runtime')
    #logger.debug("In lambda")
    response = client.post_text(
    botName='photoBot',
    botAlias="test",
    userId="test",
    inputText= q)
    print(response)
    if 'slots' in response:
        print(response['slots']['firstOb'])
        print(response['slots']['secondOb'])
        keys = [response['slots']['firstOb'],response['slots']['secondOb']]
        print(keys)
        pictures = search_intent(keys) #get images keys from elastic search labels
        if pictures == None:
            response = {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin":"*","Content-Type":"application/json"},
            "body": json.dumps([]),
            "isBase64Encoded": False}
        else:
            response = {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin":"*","Content-Type":"application/json"},
                "body": json.dumps(pictures),
                "isBase64Encoded": False
                }
    else:
        response = {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin":"*","Content-Type":"application/json"},
            "body": json.dumps([]),
            "isBase64Encoded": False}
    # #logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return response
    # return {
    #     "statusCode": 200,
    #     "isBase64Encoded": False,
    #     "body": [{
    #         'url': 'https://bb22.s3.amazonaws.com/test.jpg', 
    #         'labels': ['Dog', 'Pet', 'Animal', 'Mammal', 'Canine', 'Teeth', 'Mouth', 'Lip', 'Wolf', 'Snout', 'Red Wolf', 'Shiba', 'Cute']
    #     }, {
    #         'url': 'https://bb22.s3.amazonaws.com/WechatIMG679.jpeg', 
    #         'labels': ['Grass', 'Plant', 'Yard', 'Outdoors', 'Nature', 'Tree', 'Canine', 'Mammal', 'Animal', 'Puppy', 'Dog', 'Pet', 'Giant Panda', 'Wildlife', 'Bear', 'Person', 'Human', 'Mouth', 'Lip', 'Vegetation', 'Leaf', 'Shelter', 'Countryside', 'Building', 'Rural', 'Lawn']
    #     }, {
    #         'url': 'https://bb22.s3.amazonaws.com/test2.jpg', 
    #         'labels': ['Plant', 'Flower', 'Blossom', 'Sunflower', 'sunflower', 'indonesia'],
    #     }]
    # }



def dispatch(intent_request):
    #logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']
    return search_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

def search_intent(labels):

    url='https://search-photos-53ni23ydwcyes6ixxf6g2xrxu4.us-east-1.es.amazonaws.com/photos/_search?q='
    imgs = []
    for i in labels:
        if not i:
            break
        url2 = url+i
        response = (requests.get(url2,auth=("test1", "Test1test1!")).json())
        if "hits" not in response or "hits" not in response["hits"]: return
        results = response['hits']['hits']
        if len(results) == 0: return
        visited_imgs = {}
        for res in results:
            if "_source" not in res: continue
            res = res["_source"]
            if "objectKey" not in res or "bucket" not in res: continue
            objectKey, bucket = res["objectKey"], res["bucket"]
            print(objectKey,bucket)
            if bucket not in visited_imgs: visited_imgs[bucket] = set()
            if objectKey in visited_imgs[bucket]: continue 
            visited_imgs[bucket].add(objectKey)
            labels = res["labels"] if "labels" in res else []
            imgUrl = "https://%s.s3.amazonaws.com/%s" % (bucket, objectKey)
            imgUrl = imgUrl.replace(" ", "+")
            print(imgUrl)
            size = getSize(imgUrl)
            photo = {
                    "url": imgUrl,
                    "labels": labels,
                    "width" :size[0],
                    "height":size[1]
                }
            imgs.append(photo)
    print(imgs)
    return imgs
