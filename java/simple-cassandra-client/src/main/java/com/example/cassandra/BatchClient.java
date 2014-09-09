package com.example.cassandra;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class BatchClient extends SimpleClient {
    public BatchClient() {
    }

    public void loadData() {
        System.out.println("Loading data into the simplex keyspace.");
        long timestamp = new Date().getTime();
        // first use plain statements
        getSession().execute(
                "BEGIN BATCH USING TIMESTAMP " + timestamp +
                "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + 
                        UUID.randomUUID() + 
                        ", 'Poulaillers'' Song', 'Jamais content', 'Alain Souchon'); " +
                "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + 
                        UUID.randomUUID() + 
                        ", 'Bonnie and Clyde', 'Bonnie and Clyde', 'Serge Gainsbourg'); " +
                "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + 
                        UUID.randomUUID() + 
                        ", 'Lighthouse Keeper', 'A Clockwork Orange', 'Erika Eigen'); " +
                "APPLY BATCH"
                );
        for ( Row row : getSession().execute("SELECT * FROM simplex.songs;") ) {
            System.out.println(row.getString("title"));
        }
        // with positional binding
        PreparedStatement insertPreparedStatement = getSession().prepare(
                "BEGIN BATCH USING TIMESTAMP " + timestamp +
                "   INSERT INTO simplex.songs (id, title, album, artist) " +
                        "VALUES (?, ?, ?, ?); " +
                "   INSERT INTO simplex.songs (id, title, album, artist) " +
                        "VALUES (?, ?, ?, ?); " +
                "   INSERT INTO simplex.songs (id, title, album, artist) " +
                        "VALUES (?, ?, ?, ?); " +
                "APPLY BATCH"
             );
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        getSession().execute(
                insertPreparedStatement.bind(
                        id1, "Seaside Rendezvous", "A Night at the Opera", "Queen",
                        id2, "Entre Nous", "Permanent Waves", "Rush",
                        id3, "Frank Sinatra", "Fashion Nugget", "Cake"
                        ));
        // update those songs with some tags
        Set<String> set1 = new HashSet<String>();
        set1.add("mock-instrumental");
        Set<String> set2 = new HashSet<String>();
        set2.add("70s");
        set2.add("Canadian");
        Set<String> set3 = new HashSet<String>();
        set3.add("Sopranos");
        PreparedStatement updatePreparedStatement = getSession().prepare(
                "BEGIN BATCH" +
                        "   UPDATE simplex.songs SET tags = ? WHERE id = ?; " +
                        "   UPDATE simplex.songs SET tags = ? WHERE id = ?; " +
                        "   UPDATE simplex.songs SET tags = tags + ? WHERE id = ?; " +
                "APPLY BATCH"
                );
        getSession().execute(updatePreparedStatement.bind(
                set1, id1,
                set2, id2,
                set3, id3
                ));
        // dynamic batch statements
        BatchStatement batch = new BatchStatement();
        PreparedStatement insertSongPreparedStatement = getSession().prepare(
                "INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?);");
        batch.add(insertSongPreparedStatement.bind(
                UUID.randomUUID(), "Die Mösch", "In Gold", "Willi Ostermann"));
        batch.add(insertSongPreparedStatement.bind(
                UUID.randomUUID(), "Memo From Turner", "Performance", "Mick Jagger"));
        batch.add(insertSongPreparedStatement.bind(
                UUID.randomUUID(), "La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker"));
        getSession().execute(batch);
        // testing atomicity of the BATCH
        // NB, album column has been replaced with non-existent cd column
        UUID id4 = UUID.randomUUID();
        UUID id5 = UUID.randomUUID();
        UUID id6 = UUID.randomUUID();
        try {
            getSession().execute(
                    "BEGIN BATCH" +
                    "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + 
                            id4 + ", 'Moonlight Sonata', 'The Great German Composers', 'Ludwig van Beethoven'" +
                            "); " +
                    "   INSERT INTO simplex.songs (id, title, cd, artist) VALUES (" + 
                            id5 + ", 'Entre Nous', 'Permanent Waves', 'Rush'" +
                            "); " +
                    "   INSERT INTO simplex.songs (id, title, album, artist) VALUES (" + 
                            id6 + ", 'Die Meistersinger von Nürnberg March', 'The Great German Composers', 'Richard Wagner'" +
                            "); " +
                    "APPLY BATCH"
                    );
        } catch (InvalidQueryException iqe) {
            System.out.println("*** ERROR: " + iqe.getMessage() + " ***");
        }
        for ( Row row : getSession().execute("SELECT title, writetime(title) FROM simplex.songs;") ) {
            System.out.println(row.getString("title") + ", " + new Date(row.getLong("writetime(title)")));
        }
    }

    public static void main(String[] args) {
        BatchClient client = new BatchClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.pause();
        client.dropSchema("simplex");
        client.close();
    }

}
