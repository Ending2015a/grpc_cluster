import os
import sys
import time
import logging

import grpc
import click

from concurrent import futures

from grpc_cluster.service.auth import JWTAuthServer

@click.command()
@click.option('--port', default=6000)
@click.option('--max-workers', default=3)
def run(port, max_workers):
    server = JWTAuthServer('hello', 'world', max_workers)

    server.start(port)

    


if __name__ == '__main__':
    run()
