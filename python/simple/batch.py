#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from clients import SimpleClient

log = logging.getLogger()
log.setLevel('DEBUG')

#

class BatchClient(SimpleClient):
    def load_data(self):
        log.info('Loading data into schema.')
        

# 

def main():
    logging.basicConfig()
    client = BatchClient()
    client.connect(['127.0.0.1'])
    client.create_schema()
    
    client.pause()
    client.drop_schema('simplex')
    client.session.shutdown()

if __name__ == "__main__":
    main()

