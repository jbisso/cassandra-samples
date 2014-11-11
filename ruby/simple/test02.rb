# encoding: utf-8

require 'bundler/setup'
require 'cassandra'
require_relative 'cassandraExamples'

class BoundStatementsClient < SimpleClient
    def initialize()
        @insertSongStatement = nil
        @insertPlaylistStatement = nil
    end
    
    def prepareStatements()
        @insertSongStatement = @session.execute(
            'INSERT INTO simplex.songs ' +
                '(id, title, album, artist, tags) ' +
                'VALUES (?, ?, ?, ?, ?);')
        @insertPlaylistStatement = @session.execute(
            'INSERT INTO simplex.playlists ' +
                '(id, song_id, title, album, artist) ' +
                'VALUES (?, ?, ?, ?, ?);')
    end
    
    def loadData()
        @session.execute(
            @insertSongStatement,
            Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
            'La Petite Tonkinoise',
            'Bye Bye Blackbird',
            'Joséphine Baker',
            ['jazz', '2013'].to_set)
    end
end

client = BoundStatementsClient.new()
client.connect(['127.0.0.1'])
client.createSchema()
client.prepareStatements()
client.loadData()
client.close()

