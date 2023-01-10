import boto3
import requests
import json
import ydb
import os
import sanic.response as response
from sanic import Sanic
from sanic.response import text
from PIL import Image, ImageDraw
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
    left_top = (int(msg['vertices'][0]['x']), int(msg['vertices'][0]['y']))
    right_bottom = (int(msg['vertices'][2]['x']), int(msg['vertices'][2]['y']))

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

    draw = ImageDraw.Draw(img)
    draw.rectangle([left_top, right_bottom], outline="blue", width=8)

    image_file = BytesIO()
    img.save(image_file, format='JPEG')
    image_file.seek(0)

    face_photo = str(request.json['messages'][0]['details']['message']['message_id']) + str(photo)
    s3.upload_fileobj(image_file, faces_bucket, face_photo, ExtraArgs={'ContentType': 'image/jpeg'})

    driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
    driver.wait(fail_fast=True, timeout=5)
    pool = ydb.SessionPool(driver)

    print(f"INSERT INTO faces (id, face, name, photo) VALUES ({str(request.json['messages'][0]['details']['message']['message_id'])}, {photo}, {face_photo}")

    def add_row(session):
         return session.transaction().execute(
             f"INSERT INTO faces (id, face, name, photo) VALUES (\"{str(request.json['messages'][0]['details']['message']['message_id'])}\", \"{face_photo}\", \"\", \"{photo}\");",
                 commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                )

    pool.retry_operation_sync(add_row)

    return response.json({'body': 'Ok'}, status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ['PORT']), motd=False, access_log=False)