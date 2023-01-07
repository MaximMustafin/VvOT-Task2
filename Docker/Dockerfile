FROM python:3.10-slim

WORKDIR /app
RUN pip install boto3 Pillow requests ydb sanic
COPY ./index.py .
CMD [ "python", "index.py" ]