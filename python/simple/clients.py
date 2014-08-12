#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement
from uuid import UUID
import logging

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient(object):
    """
    A simple Cassandra client illustrating how to use the DataStax 
    Python driver.
    """
    session = None
    cluster = None

    def connect(self, nodes):
        self.cluster = Cluster(nodes, protocol_version=3)
        metadata = self.cluster.metadata
        self.session = self.cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    def create_schema(self):
        self.session.execute("""
            CREATE KEYSPACE simplex WITH replication
                = {'class':'SimpleStrategy', 'replication_factor':3};
        """)
        self.session.execute("""
            CREATE TABLE simplex.songs (
                id uuid PRIMARY KEY,
                title text,
                album text,
                artist text,
                tags set<text>,
                data blob
            );
        """)
        self.session.execute("""
            CREATE TABLE simplex.playlists (
                id uuid,
                title text,
                album text,
                artist text,
                song_id uuid,
                PRIMARY KEY (id, title, album, artist)
            );
        """)
        log.info('Simplex keyspace and schema created.')

    def load_data(self):
        self.session.execute("""
            INSERT INTO simplex.songs (id, title, album, artist, tags)
            VALUES (
                756716f7-2e54-4715-9f00-91dcbea6cf50,
                'La Petite Tonkinoise',
                'Bye Bye Blackbird',
                'Joséphine Baker',
                {'jazz', '2013'}
            );
        """)
        self.session.execute("""
            INSERT INTO simplex.songs (id, title, album, artist, tags)
            VALUES (
                f6071e72-48ec-4fcb-bf3e-379c8a696488,
                'Die Mösch',
                'In Gold',
                'Willi Ostermann',
                {'kölsch', '1996', 'birds'}
            );
        """)
        self.session.execute("""
            INSERT INTO simplex.songs (id, title, album, artist, tags)
            VALUES (
                fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
                'Memo From Turner',
                'Performance',
                'Mick Jager',
                {'soundtrack', '1991'}
            );
        """)
        self.session.execute("""
            INSERT INTO simplex.playlists (id, song_id, title, album, artist)
            VALUES (
                2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
                756716f7-2e54-4715-9f00-91dcbea6cf50,
                'La Petite Tonkinoise',
                'Bye Bye Blackbird',
                'Joséphine Baker'
            );
        """)
        self.session.execute("""
            INSERT INTO simplex.playlists (id, song_id, title, album, artist)
            VALUES (
                2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
                f6071e72-48ec-4fcb-bf3e-379c8a696488,
                'Die Mösch',
                'In Gold',
                'Willi Ostermann'
            );
        """)
        self.session.execute("""
            INSERT INTO simplex.playlists (id, song_id, title, album, artist)
            VALUES (
                3fd2bedf-a8c8-455a-a462-0cd3a4353c54,
                fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
                'Memo From Turner',
                'Performance',
                'Mick Jager'
            );
        """)
        log.info('Data loaded.')

    def query_schema(self):
        results = self.session.execute("""
            SELECT * FROM simplex.playlists
            WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;
        """)
        print "%-30s\t%-20s\t%-20s\n%s" % \
            ("title", "album", "artist",
                "-------------------------------+-----------------------+--------------------")
        for row in results:
            print "%-30s\t%-20s\t%-20s" % (row.title, row.album, row.artist)
        log.info('Schema queried.')

    def update_schema(self):
        self.session.execute("""
            UPDATE simplex.songs
            SET tags = tags + { 'entre-deux-guerres' }
            WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;
        """)
        results = self.session.execute("""
            SELECT * FROM simplex.songs
            WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;
        """)
        print "%-30s\t%-20s\t%-20s%-30s\n%s" % \
            ("title", "album", "artist",
                "tags", "-------------------------------+-----------------------+--------------------+-------------------------------")
        for row in results:
            print "%-30s\t%-20s\t%-20s%-30s" % \
                (row.title, row.album, row.artist, row.tags)
        log.info( 'Schema updated.')

    def drop_schema(self, keyspace):
        self.session.execute("DROP keyspace " + keyspace + ";")
        log.info("Dropped keyspace " + keyspace)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')
 
    def pause(self):
        wait = raw_input("Pausing execution, <CR> to continue.")
        log.info("Resuming execution.")

class BoundStatementsClient(SimpleClient):
    """
    A Cassandra client illustrating how to use prepared statements
    with the DataStax Python driver.
    """
    
    insert_song_prepared_statement = None
    insert_playlist_prepared_statement = None
    
    def prepare_statements(self):
        """
        Called once to insure that the prepared statements 
        are only prepared once per session.
        """
        self.insert_song_prepared_statement = self.session.prepare("""
            INSERT INTO simplex.songs
            (id, title, album, artist, tags)
            VALUES (?, ?, ?, ?, ?);
        """)
        self.insert_playlist_prepared_statement = self.session.prepare("""
            INSERT INTO simplex.playlists
            (id, song_id, title, album, artist)
            VALUES (?, ?, ?, ?, ?);
        """)

    def load_data(self):
        """
        Override SimpleClient's function to use bound statements.
        """
        tags = set(['jazz', '2013'])
        self.session.execute(self.insert_song_prepared_statement,
            [ UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise",
            "Bye Bye Blackbird",
            "Joséphine Baker",
            tags ]
        )
        tags = set(['1996', 'birds'])
        self.session.execute(self.insert_song_prepared_statement,
            [ UUID("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
            "Die Mösch",
            "In Gold'", 
            "Willi Ostermann",
            tags ]
        )
        tags = set(['1970', 'soundtrack'])
        self.session.execute(self.insert_song_prepared_statement,
            [ UUID("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
            "Memo From Turner",
            "Performance",
            "Mick Jager",
            tags ]
        )
        self.session.execute(self.insert_playlist_prepared_statement,
            [ UUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
            UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise",
            "Bye Bye Blackbird",
            "Joséphine Baker" ]
        )
        self.session.execute(self.insert_playlist_prepared_statement,
            [ UUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
            UUID("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
            "Die Mösch",
            "In Gold",
            "Willi Ostermann" ]
        )
        self.session.execute(self.insert_playlist_prepared_statement,
            [ UUID("3fd2bedf-a8c8-455a-a462-0cd3a4353c54"),
            UUID("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
            "Memo From Turner",
            "Performance",
            "Mick Jager" ]
        )
        log.info('BoundStatementsClient: Schema loaded.')

# callback functions for the AsynchronousExample class

def print_errors(errors):
    log.error(errors)
    
def print_results(results):
    print "%-30s\t%-20s\t%-20s%-30s\n%s" % \
        ("title", "album", "artist",
            "tags", "-------------------------------+-----------------------+--------------------+-------------------------------")
    for row in results:
        print "%-30s\t%-20s\t%-20s%-30s" % \
            (row.title, row.album, row.artist, row.tags)


class AsynchronousExample(SimpleClient):
    def query_schema(self):
        future_results =  self.session.execute_async("SELECT * FROM simplex.songs;")
        future_results.add_callbacks(print_results, print_errors)
            
# 

def main():
    logging.basicConfig()
    # client = SimpleClient()
    client = BoundStatementsClient()
    # client = AsynchronousExample()
    client.connect(['127.0.0.1'])
    client.create_schema()
    client.prepare_statements()
    client.load_data()
    client.query_schema()
    client.pause()
    client.update_schema()
    client.drop_schema('simplex')
    client.close()

if __name__ == "__main__":
    main()

