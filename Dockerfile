FROM alpine:3.6

WORKDIR /app
ADD . .

RUN apk update && \
    apt install -y libssl1.1 ca-certificates && \
    apk add python3 && \
    pip3 install -r requirements.txt

CMD python3 src/worker.py
