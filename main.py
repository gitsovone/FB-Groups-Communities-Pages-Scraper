
from multiprocessing import Process
import asyncio
import time
from typing import List
from datetime import datetime
from random import randint, choice

import mysql.connector

from new_async_parser import FBParser
from models import Worker
from config import config

def get_workers_config() -> List[tuple]:
    mysql_connection = mysql.connector.MySQLConnection(
        host = config.get('mysql_host'),
        database = config.get('mysql_database'),
        user = config.get('parser_user'),
        passwd = config.get('parser_password'),
    )
    cursor = mysql_connection.cursor()
    cursor.execute("SELECT worker, proxy, proxy_rotate_uri, type, parser_depth, activity_depth FROM workers WHERE worker > 0 AND status = 'enabled'")
    workers = [
        Worker(
            id = worker,
            proxy = {'http':proxy,'https':proxy,},
            proxy_rotate_uri = proxy_rotate_uri,
            type = type,
            depth = parser_depth,
            activity = activity_depth,
        )
        for worker, proxy, proxy_rotate_uri, type, parser_depth, activity_depth in cursor.fetchall()
    ]
    mysql_connection.close()
    return workers

def create_parser_worker(worker:Worker) -> None:

    parser = FBParser(worker)
    loop = asyncio.get_event_loop()

    if worker.type == 'parser':
        loop.run_until_complete(parser.parse_groups())
        parser.write_stat()
    elif worker.type == 'reparser':
        loop.run_until_complete(parser.reparse_posts())
        loop.run_until_complete(parser.reparse_attachments())
    elif worker.type == 'posts_reparser':
        loop.run_until_complete(parser.reparse_posts())
    elif worker.type == 'attachments_reparser':
        loop.run_until_complete(parser.reparse_attachments())
    elif worker.type == 'find_groups':
        loop.run_until_complete(parser.find_groups())
    elif worker.type == 'find_communities':
        loop.run_until_complete(parser.find_communities())
        loop.run_until_complete(parser.clean_communities())
    else:
        loop.run_until_complete(parser.parse_groups())
        loop.run_until_complete(parser.reparse_posts())
        loop.run_until_complete(parser.reparse_attachments())
        parser.write_stat()


def main():

    def create_process(worker:Worker):
        worker.process = Process(target=create_parser_worker, name=worker.id, args=(worker,))
        if not worker in workers:
            workers.append(worker)
        worker.process.start()

    designation_worker = 0
    workers = []
    workers_config = get_workers_config()
    if not workers_config:
        exit()
    
    for worker in workers_config:
        create_process(worker)

    while True:

        time.sleep(30)

        workers_config = get_workers_config()
        if not workers_config:
            for worker in workers:
                worker.process.join()
            exit()

        available_workers = []
        for worker_config in workers_config:
            available_workers.append(worker_config.id)
            new_worker = True

            for worker in workers:
                if worker.id == worker_config.id:
                    new_worker = False
                    if not worker.process.is_alive():
                        worker.type = worker_config.type
                        worker.depth = worker_config.depth
                        worker.proxy = worker_config.proxy
                        worker.proxy_rotate_uri = worker_config.proxy_rotate_uri
                        
                        create_process(worker)
            if new_worker:
                create_process(worker_config)

        if designation_worker not in available_workers:
            designation_worker = 0

if __name__ == '__main__':
    main()
