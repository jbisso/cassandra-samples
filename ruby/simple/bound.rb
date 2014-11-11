# encoding: utf-8

require 'bundler/setup'
require 'cassandra'
require_relative 'cassandraExamples'

module CassandraExamples
    class BoundStatementsClient < SimpleClient
        def initialize()
            @insertSongStatement = nil
            @insertPlaylistStatement = nil
            @songsData = [
                {
                    :id => Cassandra::Uuid.new("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                    :title  => 'La Petite Tonkinoise',
                    :album  => 'Bye Bye Blackbird',
                    :artist => 'Joséphine Baker',
                    :tags => ['jazz', '2013'].to_set)
                },
                {
                    :id     => Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
                    :title  => 'Die Mösch',
                    :album  => 'In Gold',
                    :artist => 'Willi Ostermann',
                    :tags => ['kölsch', '1996', 'birds'].to_set)
                },
                {
                    :id     => Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
                    :title  => 'Memo From Turner',
                    :album  => 'Performance',
                    :artist => 'Mick Jager',
                    :tags => ['soundtrack', '1991'].to_set)
                }
            ]
            @playlistsData = [
                {
                    :id => Cassandra::Uuid.new("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                    :song_id => Cassandra::Uuid.new("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                    :title  => 'La Petite Tonkinoise',
                    :album  => 'Bye Bye Blackbird',
                    :artist => 'Joséphine Baker'
                },
                {
                    :id => Cassandra::Uuid.new("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                    :song_id => Cassandra::Uuid.new("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
                    :title  => 'Die Mösch',
                    :album  => 'In Gold',
                    :artist => 'Willi Ostermann'
                },
                {
                    :id => Cassandra::Uuid.new("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                    :song_id => Cassandra::Uuid.new("3fd2bedf-a8c8-455a-a462-0cd3a4353c54"),
                    :title  => 'Memo From Turner',
                    :album  => 'Performance',
                    :artist => 'Mick Jager'
                }
            ]
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
            @songsData.each do |song|
                @session.execute(@insertSongStatement, song[:id], song[:title], song[:artist], song[:album], song[:tags])
            end
            @playlistsData.each do |playlist|
                @session.execute(@insertSongStatement, playlist[:id], playlist[:song_id], playlist[:title], playlist[:artist], song[:playlist])
            end
        end
    end
end

