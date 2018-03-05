#!/usr/bin/env python

import os
import json
import traceback
import logging
import configparser

from ftplib import FTP
from amqp_connection import Connection

conn = Connection()

logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(message)s",
    level=logging.DEBUG,
)

config = configparser.RawConfigParser()
config.read([
    'worker.cfg',
    '/etc/py_gpac_worker/worker.cfg'
])


def exists(ftp, path):
    try:
        ftp.nlst(path)
    except Exception as e:
        return False
    return True

def mkdirs(ftp, dst_path):
    path = ""
    for level in dst_path.split("/")[:-1]:
        if level == "":
            continue

        path = path + "/" + level
        if not exists(ftp, path):
            ftp.mkd(path)

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body.decode('utf-8'))
        logging.debug(msg)

        try:
            source = msg['parameters']['source']
            destination = msg['parameters']['destination']
            src_path = source['path']
            dst_path = destination['path']

            if 'hostname' in source:
                src_hostname = source['hostname']
                src_username = source['username']
                src_password = source['password']

                if not os.path.exists(os.path.dirname(dst_path)):
                    os.makedirs(os.path.dirname(dst_path))

                ftp = FTP(src_hostname)
                ftp.login(src_username, src_password)
                ftp.retrbinary('RETR ' + src_path, open(dst_path, 'wb').write)
                ftp.quit()
            elif 'hostname' in destination:
                dst_hostname = destination['hostname']
                dst_username = destination['username']
                dst_password = destination['password']

                ftp = FTP(dst_hostname)
                ftp.login(dst_username, dst_password)
                mkdirs(ftp, dst_path)
                ftp.storbinary('STOR ' + dst_path, open(src_path, 'rb'))
                ftp.quit()
            else:
                raise Exception("bad job order parameters")

            logging.info("""End of tranfer file from %s to %s""",
                src_path,
                dst_path)

            body_message = {
                "status": "completed",
                "job_id": msg['job_id'],
            }

            conn.sendJson('job_ftp_completed', body_message)
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            error_content = {
                "body": body.decode('utf-8'),
                "error": str(e),
                "job_id": msg['job_id'],
                "type": "job_ftp"
            }
            conn.sendJson('job_ftp_error', error_content)

    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        error_content = {
            "body": body.decode('utf-8'),
            "error": str(e),
            "type": "job_ftp"
        }
        conn.sendJson('job_ftp_error', error_content)

conn.load_configuration(config['amqp'])

queues = [
    'job_ftp',
    'job_ftp_completed',
    'job_ftp_error'
]

conn.connect(queues)
conn.consume('job_ftp', callback)
