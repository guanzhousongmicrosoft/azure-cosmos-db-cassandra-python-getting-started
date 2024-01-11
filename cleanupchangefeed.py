"""
Sample quickstart guide for Azure Cosmos DB Cassandra API
"""
import json
from asyncio.log import logger
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

import config_API as cfg


f = open('sample/error_code.json')
data = json.load(f)


def execute_command(query, values=None):
    try:
        session.execute(query, values)
        print("Query executed: " + query + str(values))
    except Exception as exception:
        if str(exception.error_code) in data:
            print(data[str(exception.error_code)])
        else:
            logger.error(exception.error_code, exception.message)


# <authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'], auth_provider=auth_provider,
                  ssl_context=ssl_context)
session = cluster.connect()

for i in range(10):
    keyspaceName = "uprofile{}".format(str(i).zfill(3))
    command = "DROP TABLE IF EXISTS {}.{};".format(keyspaceName, "change_feed_page_state")
    try:
        execute_command(command)
    except Exception as e:
        print(e)

cluster.shutdown()
