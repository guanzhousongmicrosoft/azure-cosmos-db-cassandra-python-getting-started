"""
Sample quickstart guide for Azure Cosmos DB Cassandra API
"""
import json
import random
import string
import time
import uuid
from asyncio.log import logger
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from prettytable import PrettyTable

import config_API as cfg


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


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

def execute_command_async(query, values=None):
    try:
        session.execute_async(query, values)
        print("Query executed: " + query + str(values))
    except Exception as exception:
        if str(exception.error_code) in data:
            print(data[str(exception.error_code)])
        else:
            logger.error(exception.error_code, exception.message)


def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print(t)


def getTableCount(rows):
    D = {}
    for i in range(len(rows)):
        if rows[i][0] in D:
            D[rows[i][0]].append(rows[i][1])
        else:
            D[rows[i][0]] = []
            D[rows[i][0]].append(rows[i][1])

    s = PrettyTable(['keyspace_name', 'Num_of_Tables'])
    for new_k, new_val in D.items():
        s.add_row([new_k, len([item for item in new_val if item])])
    print(s)


# <authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'], auth_provider=auth_provider,
                  ssl_context=ssl_context)
session = cluster.connect()


# </authenticateAndConnect>

def insert_row(keyspaceName):
    command_row = "INSERT INTO  {}.user  (user_id, user_name, user_bcity) VALUES (%s,%s,%s)".format(keyspaceName)
    execute_command_async(command_row, [uuid.uuid4(), randomword(10), randomword(20)])

for i in range(10):
    keyspaceName = "uprofile{}".format(str(i).zfill(3))
    print("\nCreating Keyspace {}".format(keyspaceName))

    command = "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter' : '1' }}".format(
        keyspaceName)
    execute_command(command)

    # <createTable>
    print("\nCreating Table")
    execute_command(
        'CREATE TABLE IF NOT EXISTS {}.user (user_id UUID PRIMARY KEY, user_name text, user_bcity text)'.format(
            keyspaceName))


for j in range(10000):
    for i in range(10):
        keyspaceName = "uprofile{}".format(str(i).zfill(3))
        print(j)
        insert_row(keyspaceName)
    time.sleep(0.01)

cluster.shutdown()
