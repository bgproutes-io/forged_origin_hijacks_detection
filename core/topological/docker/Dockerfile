FROM python:3.9.13-slim

RUN mkdir /tmp/db
RUN mkdir /home/main
WORKDIR /home/main

COPY ./main .


WORKDIR /home/main

RUN pip install -r requirements.txt