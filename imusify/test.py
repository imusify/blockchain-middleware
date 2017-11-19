"""
API Spec: https://github.com/imusify/blockchain-middleware/wiki/REST-API-Spec#create-wallet

Example curl commands:

    # Get Balance
    curl -vvv -H "Authorization: Bearer test-token" localhost:8090/imu/balance/your-address

    # Create a Wallet
    curl -vvv -X POST -H "Authorization: Bearer test-token" -d '{ "password": "testpwd123" }' localhost:8090/wallets/create

    # Reward
    curl -vvv -X POST -H "Authorization: Bearer test-token" -d '{ "address": "receiver-address" }' localhost:8090/imu/reward

See also:

* http://klein.readthedocs.io/en/latest/examples/nonglobalstate.html
* https://github.com/twisted/klein/blob/c5ac2e24e5f6ee0e194d3dde7a8395a5e5e70513/src/klein/app.py
* http://twistedmatrix.com/documents/12.0.0/core/howto/logging.html
"""
import os
import sys
import json
import time
import argparse
import binascii
import threading

from queue import Queue
from functools import wraps
from json.decoder import JSONDecodeError
from tempfile import NamedTemporaryFile
from collections import defaultdict

import redis
import logzero

from klein import Klein, resource
from logzero import logger

# Allow importing 'neo' from parent path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from Crypto import Random

from twisted.internet import reactor, task, endpoints
from twisted.web.server import Request, Site
from twisted.python import log

from neo.Wallets.KeyPair import KeyPair
from neo.SmartContract.Contract import Contract
from neo.Network.NodeLeader import NodeLeader
from neo.Core.Blockchain import Blockchain
from neo.Implementations.Blockchains.LevelDB.LevelDBBlockchain import LevelDBBlockchain
from neo.Settings import settings
from neo.Implementations.Wallets.peewee.UserWallet import UserWallet

from contrib.smartcontract import SmartContract

API_PORT = os.getenv("IMUSIFY_API_PORT", "8090")
PROTOCOL_CONFIG = os.path.join(parent_dir, "protocol.testnet.json")


# Setup web app
app = Klein()

def build_error(error_code, error_message, to_json=True):
    """ Builder for generic errors """
    res = {
        "errorCode": error_code,
        "errorMessage": error_message
    }
    return json.dumps(res) if to_json else res


def authenticated(func):
    """ @authenticated decorator, which makes sure the HTTP request has the correct access token """
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        # Make sure Authorization header is present
        if not request.requestHeaders.hasHeader("Authorization"):
            request.setHeader('Content-Type', 'application/json')
            request.setResponseCode(403)
            return build_error(STATUS_ERROR_AUTH_TOKEN, "Missing Authorization header")

        # Make sure Authorization header is valid
        user_auth_token = str(request.requestHeaders.getRawHeaders("Authorization")[0])
        if user_auth_token != "Bearer %s" % API_AUTH_TOKEN:
            request.setHeader('Content-Type', 'application/json')
            request.setResponseCode(403)
            return build_error(STATUS_ERROR_AUTH_TOKEN, "Wrong auth token")

        # If all good, proceed to request handler
        return func(request, *args, **kwargs)
    return wrapper


def json_response(func):
    """ @json_response decorator adds header and dumps response object """
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        res = func(request, *args, **kwargs)
        request.setHeader('Content-Type', 'application/json')
        return json.dumps(res) if isinstance(res, dict) else res
    return wrapper


def catch_exceptions(func):
    """ @catch_exceptions decorator which handles generic exceptions in the request handler """
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        try:
            res = func(request, *args, **kwargs)
        except Exception as e:
            logger.exception(e)
            request.setResponseCode(500)
            request.setHeader('Content-Type', 'application/json')
            return build_error(STATUS_ERROR_GENERIC, str(e))
        return res
    return wrapper


@app.route('/')
@authenticated
def pg_root(request):
    sc_queue.add_invoke("test", 1, 2)
    sc_queue.add_invoke("test", "x", "y")
    return 'I am the root page!'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", action="store", help="Config file (default. %s)" % PROTOCOL_CONFIG, default=PROTOCOL_CONFIG)
    args = parser.parse_args()
    settings.setup(args.config)

    logger.info("Starting api.py")
    logger.debug("Config: %s", args.config)
    logger.debug("Network: %s", settings.net_name)

    # Enable Twisted logging (see also http://twistedmatrix.com/documents/12.0.0/core/howto/logging.html)
    log.startLogging(sys.stdout)

    # # Get the blockchain up and running
    # blockchain = LevelDBBlockchain(settings.LEVELDB_PATH)
    # Blockchain.RegisterBlockchain(blockchain)

    # reactor.suggestThreadPoolSize(15)
    # NodeLeader.Instance().Start()

    # dbloop = task.LoopingCall(Blockchain.Default().PersistBlocks)
    # dbloop.start(.1)
    # Blockchain.Default().PersistBlocks()

    # Hook up Klein API to Twisted reactor
    endpoint_description = "tcp:port=%s:interface=localhost" % API_PORT
    endpoint = endpoints.serverFromString(reactor, endpoint_description)
    endpoint.listen(Site(app.resource()))

    reactor.run()
