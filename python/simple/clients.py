# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging

class SimpleClient:
    """
    A simple Cassandra client illustrating how to use the DataStax 
    Python driver.
    """

    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        print('Connected to cluster: ', metadata.cluster_name)

    def create_schema(self):
        self.session.execute('CREATE KEYSPACE simplex WITH replication ' + 
            '= {\'class\':\'SimpleStrategy\', \'replication_factor\':3};')
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
        print('Simplex keyspace and schema created.')

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
        print('Data loaded.')

    def query_schema(self):
        results = self.session.execute("""
            SELECT * FROM simplex.playlists
            WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;
        """)
        print("%-30s\t%-20s\t%-20s\n%s" %
            ("title", "album", "artist",
                "-------------------------------+-----------------------+--------------------") )
        for row in results:
            print("%-30s\t%-20s\t%-20s" % (row.title, row.album, row.artist) )

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
        print("%-30s\t%-20s\t%-20s%-30s\n%s" %
            ("title", "album", "artist",
                "tags", "-------------------------------+-----------------------+--------------------+-------------------------------") )
        for row in results:
            print("%-30s\t%-20s\t%-20s%-30s" %
                (row.title, row.album, row.artist, row.tags) )

    def drop_schema(self, keyspace):
        self.session.execute("DROP keyspace " + keyspace + ";")
        print("Dropped keyspace " + keyspace)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()


# 

def main():
    logging.basicConfig()
    client = SimpleClient()
    client.connect(['127.0.0.1'])
    client.create_schema()
    client.load_data()
    client.query_schema()
    client.update_schema()
    client.drop_schema("simplex")
    client.close()

if __name__ == "__main__":
    main()

