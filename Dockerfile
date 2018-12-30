FROM alpine:3.6

WORKDIR /app
ADD . .

RUN apk update && \
    apk add python3 libssl1.1 ca-certificates && \
    pip3 install -r requirements.txt

CMD python3 src/worker.py
