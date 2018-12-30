#!/usr/bin/env python

import configparser
import json
import logging
import os
import requests
import traceback

from ftplib import FTP, FTP_TLS
from amqp_connection import Connection

conn = Connection()

logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

config = configparser.RawConfigParser()
config.read([
    'worker.cfg',
    '/etc/py_gpac_worker/worker.cfg'
])


def exists(ftp, path):
    try:
        pwd = ftp.pwd()
        ftp.cwd(path)
        ftp.cwd(pwd)
    except Exception as e:
        print(e)
        return False
    return True


def mkdirs(ftp, prefix, dst_path):
    path = prefix
    for level in dst_path.split("/")[:-1]:
        if level == "":
            continue

        path = path + "/" + level
        if not exists(ftp, path):
            try:
                ftp.mkd(path)
            except Exception as e:
                logging.info(e)


def check_requirements(requirements):
    meet_requirements = True
    if 'paths' in requirements:
        required_paths = requirements['paths']
        assert isinstance(required_paths, list)
        for path in required_paths:
            if not os.path.exists(path):
                logging.debug("Warning: Required file does not exists: %s", path)
                meet_requirements = False

    return meet_requirements

def get_config_parameter(config, key, param):
    if key in os.environ:
        return os.environ.get(key)

    if param in config:
        return config[param]
    raise RuntimeError("Missing '" + param + "' configuration value.")

def get_parameter(parameters, key):
    for parameter in parameters:
        if parameter['id'] == key:
            value = None
            if 'default' in parameter:
                value = parameter['default']

            if 'value' in parameter:
                value = parameter['value']

            if(parameter['type'] != 'credential'):
                return value

            hostname = get_config_parameter(config['backend'], 'BACKEND_HOSTNAME', 'hostname')
            username = get_config_parameter(config['backend'], 'BACKEND_USERNAME', 'username')
            password = get_config_parameter(config['backend'], 'BACKEND_PASSWORD', 'password')

            response = requests.post(hostname + '/sessions', json={'session': {'email': username, 'password': password}})
            if response.status_code != 200:
                raise("unable to get token to retrieve credential value")

            body = response.json()
            if not 'access_token' in body:
                raise("missing access token in response to get credential value")

            headers = {'Authorization': body['access_token']}
            response = requests.get(hostname + '/credentials/' + value, headers=headers)

            if response.status_code != 200:
                raise("unable to access to credential named: " + key)

            body = response.json()
            return body['data']['value']
    return None

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body.decode('utf-8'))
        logging.debug(msg)

        try:
            parameters = msg['parameters']
            if 'requirements' in parameters:
                if not check_requirements(get_parameter(parameters, 'requirements')):
                    return False

            src_path = get_parameter(parameters, 'source_path')
            dst_path = get_parameter(parameters, 'destination_path')
            src_hostname = get_parameter(parameters, 'source_hostname')
            src_username = get_parameter(parameters, 'source_username')
            src_password = get_parameter(parameters, 'source_password')
            dst_prefix = get_parameter(parameters, 'destination_prefix')
            dst_hostname = get_parameter(parameters, 'destination_hostname')
            dst_username = get_parameter(parameters, 'destination_username')
            dst_password = get_parameter(parameters, 'destination_password')

            if src_hostname:
                if not os.path.exists(os.path.dirname(dst_path)):
                    os.makedirs(os.path.dirname(dst_path))

                ftp = FTP(src_hostname)
                ftp.login(src_username, src_password)
                ftp.retrbinary('RETR ' + src_path, open(dst_path, 'wb').write)
                ftp.quit()
            elif dst_hostname:
                ftp = FTP_TLS(dst_hostname)
                ftp.login(dst_username, dst_password)
                ftp.prot_p()
                mkdirs(ftp, dst_prefix, dst_path)
                logging.info("start upload " + src_path + " to " + dst_prefix + dst_path)
                ftp.storbinary('STOR ' + dst_prefix + dst_path, open(src_path, 'rb'))
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

            conn.publish_json('job_ftp_completed', body_message)
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            error_content = {
                "body": body.decode('utf-8'),
                "error": str(e),
                "job_id": msg['job_id'],
                "type": "job_ftp"
            }
            conn.publish_json('job_ftp_error', error_content)

    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        error_content = {
            "body": body.decode('utf-8'),
            "error": str(e),
            "type": "job_ftp"
        }
        conn.publish_json('job_ftp_error', error_content)


conn.run(
    config['amqp'],
    'job_ftp',
    [
        'job_ftp_completed',
        'job_ftp_error'
    ],
    callback
)
