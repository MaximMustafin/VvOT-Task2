import boto3
import requests
import json
import ydb
import os
import sanic.response as response
from sanic import Sanic
from sanic.response import text
from PIL import Image
from io import BytesIO


app = Sanic(__name__)


@app.after_server_start
async def after_server_start(app, loop):
    print(f"App listening at port {os.environ['PORT']}")


@app.route("", methods=['GET', 'POST'])
async def hello(request):
    print('The latest version')
    msg = json.loads(request.json['messages'][0]['details']['message']['body'])
    photo = msg['object_id']
    left = int(msg['vertices'][0]['x'])
    top = int(msg['vertices'][0]['y'])
    right = int(msg['vertices'][2]['x'])
    bottom = int(msg['vertices'][2]['y'])

    photos_bucket = 'itis-2022-2023-vvot09-photos'
    faces_bucket = 'itis-2022-2023-vvot09-faces'

    session = boto3.session.Session()
    s3 = session.client(
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name = 'ru-central1'
    )

    img_file = BytesIO()

    s3.download_fileobj(photos_bucket, photo, img_file)
    img = Image.open(img_file)

    img.crop((left, top, right, bottom)).save(photo, quality=100)

    face_photo = str(request.json['messages'][0]['details']['message']['message_id']) + str(photo)
    s3.upload_file(photo, faces_bucket, face_photo)

    ydb_client = session.client(service_name='dynamodb', endpoint_url=os.getenv('ydb_endpoint_url'),
                                aws_access_key_id=os.getenv('aws_access_key_id'),
                                aws_secret_access_key=os.getenv('aws_secret_access_key'),
                                region_name='ru-central1')

    try:
        table = ydb_client.create_table(
        TableName='faces',
        KeySchema=[
            {
            'AttributeName': 'id',
            'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
            'AttributeName': 'id',
            'AttributeType': 'S'
            },
            {
            'AttributeName': 'photo',
            'AttributeType': 'S'
            },
            {
            'AttributeName': 'face',
            'AttributeType': 'S'
            },
            {
            'AttributeName': 'name',
            'AttributeType': 'S'
            }
            ]
        )
    except Exception:
        pass

    ydb_client.put_item(TableName='faces',
                        Item=
                        {
                            'id': { 'S': str(request.json['messages'][0]['details']['message']['message_id']) },
                            'name': { 'S': ''},
                            'photo': { 'S': photo},
                            'face': { 'S': face_photo}
                        })

    return response.json({'body': 'Ok'}, status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ['PORT']), motd=False, access_log=False)