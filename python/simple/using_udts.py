#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clients import SimpleClient

from uuid import UUID

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

class UsingUDTs(SimpleClient):
    def load_data(self):
        self.session.execute(
            """
                CREATE KEYSPACE complex WITH replication
                    = {'class':'SimpleStrategy', 'replication_factor':3};
            """)
        self.session.execute(
            """
                CREATE TYPE complex.address (
                    street text,
                    city text,
                    zip_code int,
                    phones list<text>);
            """)
        self.session.execute(
            """
                CREATE TABLE complex.users (
                    id uuid PRIMARY KEY,
                    name text,
                    addresses map<text, address>);
            """)
    
    def insert_address(self):
        # register UDT
        self.cluster.register_user_type("complex", "address", Address)
        # use the UDT in a query
        self.insert_user_prepared_statement = self.session.prepare(
            """
                INSERT INTO complex.users (id, name, addresses)
                VALUES (:id, :name, :addresses);
            """)
        address = Address('123 Arnold Drive', 'Sonoma', 95476, ['707-555-1234', '800-555-9876'])
        addresses = { 'Home' : address }
        bound_statement = self.insert_user_prepared_statement.bind(
            { 'id' : UUID('756716f7-2e54-4715-9f00-91dcbea6cf50'), 'name' : 'John Doe', 'addresses' : addresses }
        )
        self.session.execute(bound_statement)

def main():
    client = UsingUDTs()
    client.connect(['127.0.0.1'])
    client.load_data()
    client.insert_address()
    client.pause()
    client.drop_schema('complex')
    client.close()

if __name__ == "__main__":
    main()


