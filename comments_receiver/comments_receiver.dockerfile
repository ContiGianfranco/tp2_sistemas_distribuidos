FROM ubuntu:20.04

RUN apt update && apt install python3 python3-pip -y
RUN pip3 install pika

COPY comments_receiver /
ENTRYPOINT ["/bin/sh"]
