FROM python:3.9.13-slim

RUN apt-get update
RUN apt-get install -y git curl pkg-config libssl-dev htop tmux vim

RUN mkdir /tmp/db
RUN mkdir /home/main
WORKDIR /home/main

COPY ./main .


WORKDIR /home/main 
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt