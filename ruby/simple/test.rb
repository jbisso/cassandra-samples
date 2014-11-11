# encoding: utf-8

require 'bundler/setup'
require 'cassandra'

class SimpleClient
    def initialize()
        @cluster = nil
        @session = nil
    end
    
    def connect(node)
        puts "Connecting to cluster."
        @cluster = Cassandra.connect(hosts: node)
        puts "Cluster: #{@cluster.name}"
        @cluster.hosts.each do |host|
          puts "Host #{host.ip}: id = #{host.id} datacenter = #{host.datacenter} rack = #{host.rack}"
        end
        @session = @cluster.connect()
    end

    def createSchema()
        @session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + 
            "= {'class':'SimpleStrategy', 'replication_factor':3};")
        @session.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" +
               "id uuid PRIMARY KEY," + 
               "title text," + 
               "album text," + 
               "artist text," + 
               "tags set<text>," + 
               "data blob" + 
            ");")
        @session.execute("CREATE TABLE IF NOT EXISTS simplex.playlists (" +
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
            "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
            "VALUES (" +
                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                "'La Petite Tonkinoise'," +
                "'Bye Bye Blackbird'," +
                "'Joséphine Baker'" +
            ");")
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

    def getExecutionInformation()
        execution = @session.execute("SELECT * FROM simplex.songs", consistency: :one).execution_info
        puts "coordinator: #{execution.hosts.last.ip}"
        puts "keyspace: #{execution.keyspace}"
        puts "cql: #{execution.statement.cql}"
        puts "requested consistency: #{execution.options.consistency}"
        puts "actual consistency: #{execution.consistency}"
        puts "number of retries: #{execution.retries}"
    end
        
    def pause(msg)
        puts msg
        line = gets
    end

    def dropSchema(keyspace)
        @session.execute("DROP KEYSPACE " + keyspace + ";")
        puts keyspace + " keyspace dropped."
    end

    def close()
        @cluster.close()
    end
end

class BoundStatementsClient < SimpleClient
    def initialize()
        @insertSongStatement = nil
        @insertPlaylistStatement = nil
    end
    
    def prepareStatements()
        @insertSongStatement = @session.prepare(
            "INSERT INTO simplex.songs " +
                "(id, title, album, artist, tags) " +
                "VALUES (?, ?, ?, ?, ?);")
        @insertPlaylistStatement = @session.prepare(
            "INSERT INTO simplex.playlists " +
                "(id, song_id, title, album, artist) " +
                "VALUES (?, ?, ?, ?, ?);")
    end
    
    def loadData()
        @session.execute(
            @insertSongStatement,
            Cassandra::Uuid.new("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise",
            "Bye Bye Blackbird",
            "Joséphine Baker",
            ['jazz', '2013'].to_set)
        @session.execute(
            @insertPlaylistStatement,
            Cassandra::Uuid.new("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
            Cassandra::Uuid.new("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise",
            "Bye Bye Blackbird",
            "Joséphine Baker")
    end
end

class AsynchronousExample < SimpleClient
    def querySchema()
        future = @session.execute_async(
            "SELECT * FROM simplex.playlists " +
            "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;")
        puts "%-30s\t%-20s\t%-20s\n%s" % 
            ['title', 'album', 'artist', '-------------------------------+-----------------------+--------------------']
        results = future.get()
        results.each do |row|
            puts "%-30s\t%-20s\t%-20s" % [ row['title'], row['album'], row['artist'] ]
        end
#        future.on_success() do |rows|
#            rows.each do |row|
#                puts "%-30s\t%-20s\t%-20s" % [ row['title'], row['album'], row['artist'] ]
#            end
#        end
    end
end

client = SimpleClient.new()
#client = BoundStatementsClient.new()
#client = AsynchronousExample.new()
client.connect(['127.0.0.1'])
client.createSchema()
#client.prepareStatements()
client.loadData()
client.querySchema()
client.getExecutionInformation()
client.pause("Continue? <CR>")
client.dropSchema("simplex")
client.close()

