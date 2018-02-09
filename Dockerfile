FROM alpine:3.6

WORKDIR /app
ADD . .

RUN apk update && \
    apk add python3 && \
    pip3 install pika amqp_connection

CMD python3 src/worker.py
