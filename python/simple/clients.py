from cassandra.cluster import Cluster
import logging

class SimpleClient:
    """
    A simple Cassandra client illustrating how to use the DataStax 
    Python driver.
    """

    session = None
    
    #def __init__(self):
        # TBD

    def connect(self, node):
        cluster = Cluster(node)
        metadata = cluster.metadata
        self.session = cluster.connect()
        print('Connected to cluster: ', metadata.cluster_name)

    def create_schema(self):
        self.session.execute('CREATE KEYSPACE simplex WITH replication ' + 
            '= {\'class\':\'SimpleStrategy\', \'replication_factor\':3};')
        self.session.execute('CREATE TABLE simplex.songs (' +
                  'id uuid PRIMARY KEY,' + 
                  'title text,' + 
                  'album text,' + 
                  'artist text,' + 
                  'tags set<text>,' + 
                  'data blob' + 
              ');')
        self.session.execute('CREATE TABLE simplex.playlists (' +
                  'id uuid,' +
                  'title text,' +
                  'album text,' +
                  'artist text,' +
                  'song_id uuid,' +
                  'PRIMARY KEY (id, title, album, artist)' +
              ');')

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()

logging.basicConfig()

client = SimpleClient()
client.connect('127.0.0.1')
client.create_schema()

client.close()

