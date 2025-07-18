FROM python:3.11.0b5-slim-bullseye

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        gcc \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install pybgproutesapi
RUN git clone https://github.com/bgproutes-io/pybgproutesapi.git /pybgproutesapi
WORKDIR /pybgproutesapi
RUN pip install .

RUN mkdir /tmp/db
RUN mkdir /home/main
WORKDIR /home/main

COPY ./main .

COPY ./docker/requirements.txt .
RUN pip install -r requirements.txt