#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement
from collections import namedtuple
from uuid import UUID
import logging
from clients import SimpleClient

log = logging.getLogger()
log.setLevel('INFO')   

# Address = namedtuple('address', ('street', 'city', 'zip_code', 'phones'))

class Address:
    street = None
    city = None
    zip_code = None
    phones = None
    
    def __init__(self, street = None, city = None, zip_code = None, phones = None):
        self.street = street
        self.city = city
        self.zip_code = zip_code
        self.phones = phones
        

class TwoOneFeatures(SimpleClient):
    insert_user_prepared_statement = None

    def create_schema(self):
        log.info('Creating schema: keyspace complex, UDT complex.address.')
        self.session.execute("""
            CREATE KEYSPACE complex WITH replication
                = {'class':'SimpleStrategy', 'replication_factor':3};
        """)
        self.session.execute("""
            CREATE TYPE complex.address (
                street text,
                city text,
                zip_code int,
                phones list<text>);
        """)
        self.session.execute("""
            CREATE TABLE complex.users (
                id uuid PRIMARY KEY,
                name text,
                addresses map<text, address>);
        """)
        self.cluster.register_user_type("complex", "address", Address)
    
    def prepare_statements(self):
        log.info('Preparing statements.')
        self.insert_user_prepared_statement = self.session.prepare("""
            INSERT INTO complex.users (id, name, addresses)
                VALUES (:id, :name, :addresses);
        """)
        
    def load_data(self):
        log.info('Loading data into schema.')
        address = Address('123 Arnold Drive', 'Sonoma', 95476, ['707-555-1234', '800-555-9876'])
        addresses = {'Home' : address }
        bound_statement = self.insert_user_prepared_statement.bind(
            { 'id' : UUID('756716f7-2e54-4715-9f00-91dcbea6cf50'), 'name' : 'John Doe', 'addresses' : addresses }
        )
        self.session.execute(bound_statement)

# 

def main():
    logging.basicConfig()
    client = TwoOneFeatures()
    client.connect(['ec2-54-176-125-19.us-west-1.compute.amazonaws.com'])
    client.create_schema()
    client.prepare_statements()
    client.load_data()
    #client.query_schema()
    client.pause()
    #client.update_schema()
    client.drop_schema('complex')
    #client.close()

if __name__ == "__main__":
    main()

