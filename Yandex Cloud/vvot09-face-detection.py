import boto3
import os
import io
import json
import requests
import base64
import sys

def handler(event, context):
    session = boto3.session.Session()

    s3 = session.client(
        aws_access_key_id=os.getenv('aws_access_key_id'), 
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

    bucket_id = event['messages'][0]['details']['bucket_id']
    object_id = event['messages'][0]['details']['object_id']
    img = io.BytesIO()
    s3.download_fileobj(bucket_id, object_id, img)

    if not sys.getsizeof(img) / 1024 / 1024 <= 1:
        print('Image size must be less than 1 MB - ', sys.getsizeof(img) / 1024 / 1024)
        return {
            'status_code': 200,
            'body':'Image size must be less than 1 MB',
        }
    print(sys.getsizeof(img) / 1024 / 1024)
    base64_img = base64.b64encode(img.getbuffer().tobytes())


    yv_response = requests.post("https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze",
                        data=json.dumps({
                                "folderId": os.getenv('folder_id'),
                                "analyze_specs": [{
                                    "content": base64_img.decode("UTF-8"),
                                    "features": [{
                                        "type": "FACE_DETECTION"
                                    }]
                                }]
                            }
                        ), 
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer " + os.getenv('iam_token') 
                        })

    yv_json = yv_response.json()

    if yv_json['results'][0]['results'][0]['faceDetection']:
        queue_client = boto3.client(
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key'),
            service_name='sqs',
            endpoint_url='https://message-queue.api.cloud.yandex.net',
            region_name='ru-central1'
        )
        queue_url = queue_client.get_queue_url(QueueName=os.getenv('queue_name'))['QueueUrl']
        
        for face in yv_json['results'][0]['results'][0]['faceDetection']['faces']:
            queue_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({
                        'object_id': object_id,
                        'vertices': face['boundingBox']['vertices']
                    })
                )
            print('Sent message to queue ', os.getenv('queue_name'))
    else:
        print('There are no faces')
        return {
            'status_code': 200,
            'body':'There are no faces',
        }

    return {
            'status_code': 200,
            'body':'Done',
        }