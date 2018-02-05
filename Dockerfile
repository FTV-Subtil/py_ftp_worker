FROM alpine:3.6

WORKDIR /app
ADD . .

RUN apk update
RUN apk add python3
RUN pip3 install pika

CMD python3 src/worker.py
