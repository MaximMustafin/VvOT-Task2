import json
import requests
import os
import ydb
import io
import boto3

driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
driver.wait(fail_fast=True, timeout=5)
pool = ydb.SessionPool(driver)

session = boto3.session.Session(region_name='ru-central1')
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net')

def get_face(session):
    void_text = ''
    return session.transaction().execute(
        f'SELECT face FROM faces WHERE name=\"{void_text}\";',
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )


text = ""
def handler(event, context):
    request_body = json.loads(event['body'])
    request_text = ""
    try:
        request_text = request_body['message']['text']
    except:
        response_body = {
            "method": "sendMessage",
            "chat_id": request_body['message']['chat']['id'],
            "text": "Error"
        };

    try:
        if request_body['message']['reply_to_message']['caption'] is not None:
            face = request_body['message']['reply_to_message']['caption']
            name = request_body['message']['text']

            def add_name(session):
                return session.transaction().execute(
                    f'UPDATE faces SET name=\"{name}\" WHERE face=\"{face}\";',
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                )

            pool.retry_operation_sync(add_name)
            response_body = {
                "method": "sendMessage",
                "chat_id": request_body['message']['chat']['id'],
                "text": "Имя фото лица изменено"
            };
    except Exception as e:
        print(e)
        if request_text == "/getface":
            try:
                key = pool.retry_operation_sync(get_face)[0].rows[0].face.decode("utf-8") 
                photo = f"{os.getenv('API_GATEWAY')}/?face={key}"
                print(photo)
                response_body = {
                    "method": "sendPhoto",
                    "chat_id": request_body['message']['chat']['id'],
                    "photo": photo,
                    "caption": key
                };
            except Exception as e:
                print(e)
                response_body = {
                    "method": "sendMessage",
                    "chat_id": request_body['message']['chat']['id'],
                    "text": 'У всех фото лиц есть имена'
                };

        elif request_text.startswith("/find"):
            try:
                name = "".join(request_text.split("/find ")[1:])
                print('Имя', name)

                def get_photos(session):
                    return session.transaction().execute(
                        f'SELECT photo FROM faces WHERE name=\"{name}\";',
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                    )

                photos_names = pool.retry_operation_sync(get_photos)[0].rows
                photos = []
                if len(photos_names) == 0:
                    response_body = {
                        "method": "sendMessage",
                        "chat_id": request_body['message']['chat']['id'],
                        "text": "Фото лица для этого имени не найдено"
                    };
                else:
                    for i in photos_names:
                        text = i.get('photo').decode("utf-8") 
                        photos.append(
                            {"type": "photo",
                            "media": f"{os.getenv('API_GATEWAY')}/photo?name={text}"})

                    response_body = {
                        "method": "sendMediaGroup",
                        "chat_id": request_body['message']['chat']['id'],
                        "media": photos
                    };
                if request_text == "/find":
                    text = "format: /find {name}"
                    response_body = {
                        "method": "sendMessage",
                        "chat_id": request_body['message']['chat']['id'],
                        "text": text
                    };
            except:
                text = "incorrect message format"
                response_body = {
                    "method": "sendMessage",
                    "chat_id": request_body['message']['chat']['id'],
                    "text": text
                };

        else:
            text = "Команды:\n /getface - Присылает фото лица без имени. Чтобы присвоить имя фото лица, необходимо в ответ на фото отправить имя.\n /find {name} - Присылает все фотографии с именем {name}."
            response_body = {
                "method": "sendMessage",
                "chat_id": request_body['message']['chat']['id'],
                "text": text
            };
        

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'isBase64Encoded': False,
        'body': json.dumps(response_body)
    }