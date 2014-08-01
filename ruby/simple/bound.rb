# encoding: utf-8

require 'bundler/setup'
require 'cql'
require_relative 'cassandraExamples'

module CassandraExamples
    class BoundStatementsClient < SimpleClient
        def initialize()
            @insertSongStatement = nil
            @insertPlaylistStatement = nil
        end
        
        def prepareStatements()
            @insertSongStatement = @session.execute(
                "INSERT INTO simplex.songs " +
                    "(id, title, album, artist, tags) " +
                    "VALUES (?, ?, ?, ?, ?);")
            @insertPlaylistStatement = @session.execute(
                "INSERT INTO simplex.playlists " +
                    "(id, song_id, title, album, artist) " +
                    "VALUES (?, ?, ?, ?, ?);")
        end
    end
end

