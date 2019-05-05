#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from datasource.tushare_impl import TushareImpl
from datasource.datafile_impl import DatafileImpl
import logging
import socket
import time
import threading
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(levelname)s][%(threadName)s][%(message)s]')
logger = logging.getLogger(__name__)

'''
TODO: produce stream data (could use kafka)
'''

bind_ip = '0.0.0.0'
bind_port = 5003


def startup():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((bind_ip, bind_port))
    server.listen(5)
    logger.info(f"listening on {bind_ip}:{bind_port}")
    return server


def handle_client(client_socket):
    # req = client_socket.recv(1024)
    # thread_name = threading.current_thread().getName()
    logger.info(f"start sending...")
    formant = '%Y-%m-%d %H:%M:%S'
    now = None
    send_now = ""
    with open('../../data/997stock_3day_tick_data_sortby_time.csv', 'r') as f:
        while True:
            s = f.readline()
            current_time = s.split(",")[0]
            if current_time == 'time':
                continue
            else:
                t = dt.strptime(current_time, formant)
            if now is None:
                now = t
                send_now += s
            elif now == t:
                send_now += s
            elif now != t:
                v = len(send_now.split("\n"))
                logger.info(f"send - {v} rows")
                try:
                    client_socket.send(send_now.encode('utf-8'))
                except Exception as e:
                    logger.info('lost conn.')
                    return
                time.sleep(5)
                send_now = ""
                now = t


if __name__ == "__main__":
    # test
    # ds_tushare = TushareImpl()
    server = startup()
    while True:
        client, addr = server.accept()
        logger.info("accept connection")
        threading.Thread(target=handle_client, args=(client, )).start()


