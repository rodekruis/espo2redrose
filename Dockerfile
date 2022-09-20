FROM python:3.9-slim-bullseye

ADD credentials /credentials
ADD data /data

WORKDIR /pipeline
ADD pipeline .
RUN pip install .
