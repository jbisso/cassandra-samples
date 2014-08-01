# encoding: utf-8

require 'bundler/setup'
require 'cql'

module CassandraExamples
    class SimpleClient
        def initialize()
            @cluster = nil
            @session = nil
        end
        
        def connect(node)
            puts "Connecting to cluster."
            @cluster = Cql.cluster
                .with_contact_points(node)
                .build
            @cluster.hosts.each do |host|
              puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
            end
            @session = @cluster.connect()
        end
        
        def createSchema()
            @session.execute("CREATE KEYSPACE simplex WITH replication " + 
                "= {'class':'SimpleStrategy', 'replication_factor':3};")
            @session.execute("CREATE TABLE simplex.songs (" +
                   "id uuid PRIMARY KEY," + 
                   "title text," + 
                   "album text," + 
                   "artist text," + 
                   "tags set<text>," + 
                   "data blob" + 
                ");")
            @session.execute("CREATE TABLE simplex.playlists (" +
                   "id uuid," +
                   "title text," +
                   "album text, " + 
                   "artist text," +
                   "song_id uuid," +
                   "PRIMARY KEY (id, title, album, artist)" +
                ");")
            puts "Simplex keyspace and schema created."
        end
        
        def loadData()
            @session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                    "'La Petite Tonkinoise'," +
                    "'Bye Bye Blackbird'," +
                    "'Joséphine Baker'," +
                    "{'jazz', '2013'})" +
                ";")
            @session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                    "'Die Mösch'," +
                    "'In Gold'," +
                    "'Willi Ostermann'," +
                    "{'kölsch', '1996', 'birds'}" +
                ");")
            @session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                    "'Memo From Turner'," +
                    "'Performance'," +
                    "'Mick Jager'," +
                    "{'soundtrack', '1991'}" +
                ");")
            @session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                    "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                    "'La Petite Tonkinoise'," +
                    "'Bye Bye Blackbird'," +
                    "'Joséphine Baker'" +
                ");")
            @session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                    "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                    "'Die Mösch'," +
                    "'In Gold'," +
                    "'Willi Ostermann'" +
                ");")
            @session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "3fd2bedf-a8c8-455a-a462-0cd3a4353c54," +
                    "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                    "'Memo From Turner'," +
                    "'Performance'," +
                    "'Mick Jager'" +
                ");")
            puts "Data loaded."
        end
        
        def querySchema()
            results = @session.execute(
                "SELECT * FROM simplex.playlists " +
                "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;")
            puts "%-30s\t%-20s\t%-20s\n%s" % 
                ['title', 'album', 'artist', '-------------------------------+-----------------------+--------------------']
            results.each do |row|
                puts "%-30s\t%-20s\t%-20s" % [ row['title'], row['album'], row['artist'] ]
            end
        end
        
        def updateSchema()
            @session.execute(
                "UPDATE simplex.songs " +
                "SET tags = tags + { 'entre-deux-guerres' } " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;")
            results = @session.execute(
                "SELECT * FROM simplex.songs " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;")
            puts "%-30s\t%-20s\t%-20s%-30s\n%s" % 
                ['title', 'album', 'artist', 'tags', '-------------------------------+-----------------------+--------------------+-------------------------------']
            results.each do |row|
                puts "%-30s\t%-20s\t%-20s" % [ row['title'], row['album'], row['artist'], row['tags'].inspect ]
            end
        end
        
        def dropSchema(keyspace)
            @session.execute("DROP KEYSPACE " + keyspace + ";")
            puts keyspace + " keyspace dropped."
        end
        
        def pause(msg)
            puts msg
            line = gets
        end
    
        def close()
            @cluster.close()
        end
    end
end


