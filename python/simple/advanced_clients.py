#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.policies import DowngradingConsistencyRetryPolicy
from cassandra.policies import ConstantReconnectionPolicy

import logging

log = logging.getLogger()
log.setLevel('DEBUG')

class RollYourOwnCluster:
    cluster = None
    session = None
    
    def __init__(self):
        self.cluster = Cluster(
            contact_points=['127.0.0.1', '127.0.0.2'],
            default_retry_policy=DowngradingConsistencyRetryPolicy(),
            reconnection_policy=ConstantReconnectionPolicy(20.0, 10)
        )
        self.session = self.cluster.connect()

# 

def main():
    logging.basicConfig()
    client = RollYourOwnCluster()
    print('Connected to cluster: ' + client.cluster.metadata.cluster_name)
    client.session.shutdown()

if __name__ == "__main__":
    main()

