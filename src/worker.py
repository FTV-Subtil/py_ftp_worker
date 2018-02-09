#!/usr/bin/env python

import os
import json
import traceback
import logging

from ftplib import FTP

from amqp_connection import Connection

conn = Connection()

logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(message)s",
    level=logging.DEBUG,
)

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body.decode('utf-8'))
        logging.debug(msg)

        try:
            source = msg['parameters']['source']
            destination = msg['parameters']['destination']
            src_path = source['path']
            dst_path = destination['path']

            src_hostname = source['hostname']
            src_username = source['username']
            src_password = source['password']

            if not os.path.exists(os.path.dirname(dst_path)):
                os.makedirs(os.path.dirname(dst_path))

            ftp = FTP(src_hostname)
            ftp.login(src_username, src_password)
            ftp.retrbinary('RETR ' + src_path, open(dst_path, 'wb').write)
            ftp.quit()

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
            conn.sendJson('error', error_content)

    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        error_content = {
            "body": body.decode('utf-8'),
            "error": str(e),
            "type": "job_ftp"
        }
        conn.sendJson('error', error_content)

conn.load_configuration()

queues = [
    'job_ftp',
    'job_ftp_completed',
    'error'
]

conn.connect(queues)
conn.consume('job_ftp', callback)
