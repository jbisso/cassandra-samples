package com.example.cassandra;

import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * A simple client application that illustrates connecting to
 * a Cassandra cluster. retrieving metadata, creating a schema,
 * loading data into it, and then querying it.
 */
public class SimpleClient {
   private Session session;

   public SimpleClient() {
   }

   /**
    * Connects to the specified node.
    * @param node a host name or IP address of the node in the cluster
    */
   public void connect(String node) {
      Cluster cluster = Cluster.builder()
            .addContactPoint(node)
            // .withSSL() // uncomment if using client to node encryption
            .build();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      session = cluster.connect();
   }
   
   /**
    * Creates the simplex keyspace and two tables, songs and playlists.
    */
   public void createSchema() {
      session.execute(
            "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + 
            "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};");
      // create songs and playlist tables
      session.execute(
            "CREATE TABLE IF NOT EXISTS simplex.songs (" +
                  "id uuid PRIMARY KEY," + 
                  "title text," + 
                  "album text," + 
                  "artist text," + 
                  "tags set<text>," + 
                  "data blob" + 
              ");");
      session.execute(
            "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
                  "id uuid," +
                  "title text," +
                  "album text, " + 
                  "artist text," +
                  "song_id uuid," +
                  "PRIMARY KEY (id, title, album, artist)" +
                  ");");
      System.out.println("Simplex keyspace and schema created.");
   }
   
   /**
    * Loads some data into the schema so that we can query the tables.
    */
   public void loadData() {
      // insert data in the tables
      session.execute(
            "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
            "VALUES (" +
                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                "'La Petite Tonkinoise'," +
                "'Bye Bye Blackbird'," +
                "'Joséphine Baker'," +
                "{'jazz', '2013'})" +
            ";");
      session.execute(
            "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
            "VALUES (" +
                "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                "'Die Mösch'," +
                "'In Gold'," +
                "'Willi Ostermann'," +
                "{'kölsch', '1996', 'birds'}" +
            ");");
      session.execute(
            "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
            "VALUES (" +
                "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                "'Memo From Turner'," +
                "'Performance'," +
                "'Mick Jager'," +
                "{'soundtrack', '1991'}" +
            ");");
      session.execute(
            "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
            "VALUES (" +
                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                "'La Petite Tonkinoise'," +
                "'Bye Bye Blackbird'," +
                "'Joséphine Baker'" +
            ");");
      session.execute(
            "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
            "VALUES (" +
                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                "'Die Mösch'," +
                "'In Gold'," +
                "'Willi Ostermann'" +
            ");");
      session.execute(
            "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
            "VALUES (" +
                "3fd2bedf-a8c8-455a-a462-0cd3a4353c54," +
                "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                "'Memo From Turner'," +
                "'Performance'," +
                "'Mick Jager'" +
            ");");
      System.out.println("Data loaded.");
   }
   
   /**
    * Queries the songs and playlists tables for data.
    */
   public void querySchema() {
      ResultSet results = session.execute(
            "SELECT * FROM simplex.playlists " +
            "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
      System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
            "-------------------------------+-----------------------+--------------------"));
      for (Row row : results) {
         System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
               row.getString("album"), row.getString("artist")));
      }
      System.out.println();
   }
   
   /**
    * Updates the songs table with a new song and then queries the table
    * to retrieve data.
    */
   public void updateSchema() {
      session.execute(
            "UPDATE simplex.songs " +
            "SET tags = tags + { 'entre-deux-guerres' } " +
            "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
      
      ResultSet results = session.execute(
            "SELECT * FROM simplex.songs " +
            "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
      
      System.out.println(String.format("%-30s\t%-20s\t%-20s%-30s\n%s", "title", "album", "artist",
            "tags", "-------------------------------+-----------------------+--------------------+-------------------------------"));
      for (Row row : results) {
         System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
               row.getString("album"),  row.getString("artist"), row.getSet("tags", String.class)));
      }
   }

   /**
    * Drops the specified schema.
    * @param keyspace the keyspace to drop (and all of its data)
    */
   public void dropSchema(String keyspace) {
      getSession().execute("DROP KEYSPACE " + keyspace);
      System.out.println("Finished dropping " + keyspace + " keyspace.");
   }

   /**
    * Returns the current session.
    * @return the current session to execute statements on
    */
   public Session getSession() {
      return this.session;
   }
   
   // used by the workaround method in the BoundStatementsclient child class.
   void setSession(Session session) {
   	this.session = session;
   }
   
   public void pause() {
       pause("Press <CR> to continue.");
   }
       
   public void pause(String message) {
       System.out.println(message);
       try {
           System.in.read();
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   /**
    * Shuts down the session and its cluster.
    */
   public void close() {
      session.close();
      session.getCluster().close();
   }

   /**
    * Creates  simple client application that illustrates connecting to
    * a Cassandra cluster. retrieving metadata, creating a schema,
    * loading data into it, and then querying it.
    * @param args ignored
    */
   public static void main(String[] args) {
      SimpleClient client = new SimpleClient();
      client.connect("127.0.0.1");
      client.createSchema();
      client.loadData();
      client.querySchema();
      client.updateSchema();
      client.pause();
      client.dropSchema("simplex");
      client.close();
   }
}
