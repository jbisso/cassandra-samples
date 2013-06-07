package com.example.cassandra;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class BoundStatementsClient extends SimpleClient {
	public BoundStatementsClient() {
	}

	public void loadData() {
      PreparedStatement statement = getSession().prepare(
            "INSERT INTO songs " +
            "(id, title, album, artist, tags) " +
            "VALUES (?, ?, ?, ?, ?);");
      BoundStatement boundStatement = new BoundStatement(statement);
      Set<String> tags = new HashSet<String>();
      tags.add("jazz");
      tags.add("2013");
      getSession().execute(boundStatement.bind(
            UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise'",
            "Bye Bye Blackbird'",
            "Joséphine Baker",
            tags ) );
      tags = new HashSet<String>();
      tags.add("1996");
      tags.add("birds");
      getSession().execute(boundStatement.bind(
            UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
            "Die Mösch",
            "In Gold'", 
            "Willi Ostermann",
            tags) );
      tags = new HashSet<String>();
      tags.add("1970");
      tags.add("soundtrack");
      getSession().execute(boundStatement.bind(
            UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
            "Memo From Turner",
            "Performance",
            "Mick Jager",
            tags) );
      // playlists table
      statement = getSession().prepare(
            "INSERT INTO playlists " +
            "(id, song_id, title, album, artist) " +
            "VALUES (?, ?, ?, ?, ?);");
      boundStatement = new BoundStatement(statement);
      getSession().execute(boundStatement.bind(
            UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
            UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise",
            "Bye Bye Blackbird",
            "Joséphine Baker") );
      getSession().execute(boundStatement.bind(
            UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
            UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
            "Die Mösch",
            "In Gold",
            "Willi Ostermann") );
      getSession().execute(boundStatement.bind(
            UUID.fromString("3fd2bedf-a8c8-455a-a462-0cd3a4353c54"),
            UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
            "Memo From Turner",
            "Performance",
            "Mick Jager") );
   }
   
   // See https://issues.apache.org/jira/browse/CASSANDRA-5468
   // for now create a new session that explicitly connects with a keypsace specified
   void workaround() {
		Session session = getSession();
		setSession( session.getCluster().connect("simplex") );
		session.shutdown();
   }

   public static void main(String[] args) {
		BoundStatementsClient client = new BoundStatementsClient();
		client.connect("127.0.0.1");
		client.createSchema();
		client.workaround();
		client.loadData();
      client.querySchema();
      client.updateSchema();
      client.dropSchema("simplex");
      client.close();
	}
}
