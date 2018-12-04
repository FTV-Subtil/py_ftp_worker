FROM alpine:3.6

WORKDIR /app
ADD . .

RUN apk update && \
    apk add python3 && \
    pip install --upgrade pip && \
    pip3 install -r requirements.txt

CMD python3 src/worker.py
