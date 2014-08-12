#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clients import SimpleClient

class TestTuples(SimpleClient):
    def create_schema(self):
        super(TestTuples, self).create_schema()
        self.session.execute(
            """
                CREATE TABLE simplex.tuple_test (
                    the_key int PRIMARY KEY,
                    the_tuple tuple<int, text, float>)
            """)
        
    def insert_tuple(self):
        the_tuple = (0, 'abc', 1.0)
        prepared = self.session.prepare("INSERT INTO simplex.tuple_test(the_key, the_tuple) VALUES (?, ?);")
        self.session.execute(prepared, parameters=(1, the_tuple))

def main():
   client = TestTuples()
   client.connect(['127.0.0.1'])
   client.create_schema()
   client.insert_tuple()

if __name__ == "__main__":
    main()

