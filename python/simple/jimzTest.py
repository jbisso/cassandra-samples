#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement
from uuid import UUID

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    

if __name__ == "__main__":
    main()

