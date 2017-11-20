"""
API Spec: https://github.com/imusify/blockchain-middleware/wiki/REST-API-Spec#create-wallet

Example curl commands:

    # Get Balance
    curl -vvv -H "Authorization: Bearer test-token" localhost:8090/imu/balance/AbacVZYsiBi8kWGBkeq8fbgoTqwQFrj638

    # Create a Wallet
    curl -vvv -X POST -H "Authorization: Bearer test-token" -d '{ "password": "testpwd123" }' localhost:8090/wallets/create

    # Reward
    curl -vvv -X POST -H "Authorization: Bearer test-token" -d '{ "address": "AbacVZYsiBi8kWGBkeq8fbgoTqwQFrj638" }' localhost:8090/imu/reward

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
import logging

from functools import wraps
from json.decoder import JSONDecodeError
from tempfile import NamedTemporaryFile
from collections import defaultdict

import logzero
import redis

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
from twisted.internet.protocol import Factory

from neo.Wallets.KeyPair import KeyPair
from neo.SmartContract.Contract import Contract
from neo.Network.NodeLeader import NodeLeader
from neo.Implementations.Blockchains.LevelDB.LevelDBBlockchain import LevelDBBlockchain
from neo.Core.Blockchain import Blockchain
from neo.Settings import settings

from imusmartcontract import ImuSmartContract


# Set constants from env vars or use default
API_PORT = os.getenv("IMUSIFY_API_PORT", "8090")
CONTRACT_HASH = os.getenv("IMUSIFY_SC_HASH", "645c4dcdc6cc4b69a8b8d7c0a532795d0312d5e6")

PROTOCOL_CONFIG = os.path.join(parent_dir, "shared-privnet.json")
WALLET_FILE = os.getenv("IMUSIFY_WALLET_FILE", os.path.join(parent_dir, "neo-privnet.wallet"))
WALLET_PWD = os.getenv("IMUSIFY_WALLET_PWD", "coz")

# PROTOCOL_CONFIG = os.path.join(parent_dir, "protocol.testnet.json")
# WALLET_FILE = os.getenv("IMUSIFY_WALLET_FILE", os.path.join(parent_dir, "imusify-testnet.wallet"))
# WALLET_PWD = os.getenv("IMUSIFY_WALLET_PWD", "asdasdasdasd")

print(PROTOCOL_CONFIG, API_PORT, CONTRACT_HASH, WALLET_FILE, WALLET_PWD)

LOGFILE = os.path.join(parent_dir, "imusify.log")
logzero.logfile(LOGFILE, maxBytes=1e7, backupCount=3)

# API error codes
STATUS_ERROR_AUTH_TOKEN = 1
STATUS_ERROR_JSON = 2
STATUS_ERROR_GENERIC = 3

IS_DEV = True
API_AUTH_TOKEN = os.getenv("IMUSIFY_API_AUTH_TOKEN")
if not API_AUTH_TOKEN:
    if IS_DEV:
        API_AUTH_TOKEN = "test-token"
    else:
        raise Exception("No IMUSIFY_API_AUTH_TOKEN environment variable found")

redis_cache = redis.StrictRedis(host='localhost', port=6379, db=0)

# Setup the smart contract
smart_contract = ImuSmartContract(CONTRACT_HASH, WALLET_FILE, WALLET_PWD)

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
    return 'I am the root page!'


@app.route('/imu/balance/<address>')
@catch_exceptions
@authenticated
@json_response
def imu_get_balance(request, address):
    if len(address) < 34:
        logger.warn("Wallet address '%s' is not 34 characters" % address)
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "Address not 34 characters")

    r_balance = redis_cache.get("balance:%s" % address)
    r_updated_at = redis_cache.get("balanceUpdatedAt:%s" % address)
    r_invoked_at = redis_cache.get("invokedAt:%s" % address)

    balance = int(r_balance) if r_balance is not None else r_balance
    updated_at = int(r_updated_at) if r_updated_at is not None else r_updated_at
    invoked_at = int(r_invoked_at) if r_invoked_at is not None else r_invoked_at

    logger.info("balance: %s (%s)" % (balance, type(balance)))
    logger.info("updated_at: %s (%s)" % (updated_at, type(updated_at)))
    logger.info("invoked_at: %s (%s)" % (invoked_at, type(invoked_at)))
    seconds_since = int(time.time()) - invoked_at if invoked_at else None
    logger.info("$IMU balance of address '%s': %s (%s sec ago)" % (address, balance, seconds_since))

    # After 10 minutes, re-query balance
    # if not seconds_since or seconds_since > 60 * 10:
    # # if True:
    #     # invoke smart contract
    #     address_bytes = binascii.hexlify(address.encode())
    #     sc_queue.add_invoke("balanceOf", address_bytes)
    #     redis_cache.set("invokedAt:%s" % address, int(time.time()))

    # Return result
    return {
        "address": address,
        "balance": balance,
        "updatedAt": updated_at
    }


@app.route('/wallets/create', methods=['POST'])
@catch_exceptions
@authenticated
@json_response
def create_wallet(request):
    try:
        body = json.loads(request.content.read().decode("utf-8"))
    except JSONDecodeError as e:
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "JSON Error: %s" % str(e))

    # Fail if not a password
    if not "password" in body:
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "No password in request body.")

    # Fail if no good password
    pwd = body["password"]
    if len(pwd) < 8:
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "Password needs a minimum length of 8 characters.")

    private_key = bytes(Random.get_random_bytes(32))
    key = KeyPair(priv_key=private_key)

    return {
        "address": key.GetAddress(),
        "nep2_key": key.ExportNEP2(pwd)
    }


# /imu/send transfers some tokens from the initial contract wallet to some user address
@app.route('/imu/reward', methods=['POST'])
@authenticated
@catch_exceptions
@json_response
def imu_reward(request):
    try:
        body = json.loads(request.content.read().decode("utf-8"))
    except JSONDecodeError as e:
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "JSON Error: %s" % str(e))

    # Fail if all fields provided
    if "address" not in body:
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "Missing address.")

    address = body["address"]
    if len(address) < 34:
        logger.warn("Wallet address '%s' is not 34 characters" % address)
        request.setResponseCode(400)
        return build_error(STATUS_ERROR_JSON, "Address not 34 characters")

    logger.info("Reward %s with $IMU" % address)

    # Call smart contract
    address_bytes = binascii.hexlify(address.encode())
    smart_contract.add_invoke("reward", address_bytes)

    return {}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", action="store", help="Config file (default. %s)" % PROTOCOL_CONFIG, default=PROTOCOL_CONFIG)
    args = parser.parse_args()
    settings.setup(args.config)

    logger.info("Starting api.py")
    logger.info("Config: %s", args.config)
    logger.info("Network: %s", settings.net_name)

    # Enable Twisted logging (see also http://twistedmatrix.com/documents/12.0.0/core/howto/logging.html)
    # log.startLogging(sys.stdout)
    # logging.getLogger('twisted').setLevel(logging.CRITICAL)

    # Get the blockchain up and running
    blockchain = LevelDBBlockchain(settings.LEVELDB_PATH)
    Blockchain.RegisterBlockchain(blockchain)
    reactor.suggestThreadPoolSize(15)
    NodeLeader.Instance().Start()
    dbloop = task.LoopingCall(Blockchain.Default().PersistBlocks)
    dbloop.start(.1)
    Blockchain.Default().PersistBlocks()

    # Hook up Klein API to Twisted reactor
    endpoint_description = "tcp:port=%s:interface=localhost" % API_PORT
    endpoint = endpoints.serverFromString(reactor, endpoint_description)
    endpoint.listen(Site(app.resource()))

    # Start the smart contract thread
    smart_contract.start()

    # helper for periodic log output
    def log_infos():
        while True:
            logger.info("Block %s / %s", str(Blockchain.Default().Height), str(Blockchain.Default().HeaderHeight))
            time.sleep(60)

    t = threading.Thread(target=log_infos)
    t.setDaemon(True)
    t.start()

    # reactor.callInThread(sc_queue.run)
    reactor.run()
