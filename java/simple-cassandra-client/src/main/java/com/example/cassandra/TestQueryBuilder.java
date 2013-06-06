package com.example.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class TestQueryBuilder extends SimpleClient {
   public TestQueryBuilder() {
   }
   
   public void createSchema() {
      getSession().execute("CREATE KEYSPACE simplex WITH " 
            + "replication = {'class':'SimpleStrategy', 'replication_factor':3};");
      getSession().execute("CREATE TABLE simplex.users (id uuid PRIMARY KEY, "
               + "name text, locations map<timestamp, text>);");
      Insert insert = QueryBuilder.insertInto("users")
            .value("id", UUID.randomUUID())
            .value("name", "Paul Tepley");
      Map<String, String> locations = new HashMap<String, String>();
      locations.put("2001-04-26 20:35", "San Diego, CA");
      locations.put("2009-02-11 09:29", "Boulder, CO");
      insert.value("locations", locations);
      getSession().execute(insert);
      
      insert = QueryBuilder.insertInto("users")
            .value("id", UUID.randomUUID())
            .value("name", "Tom Upton");
      locations = new HashMap<String, String>();
      locations.put("2000-12-24 12:32",  "Austin, TX");
      insert.value("locations",  locations);
      getSession().execute(insert);
      insert = QueryBuilder.insertInto("users");
      insert.value("id", UUID.randomUUID());
      insert.value("name", "Jim Bisso");
      locations = new HashMap<String, String>();
      locations.put("1993-03-01 00:00",  "Richmond, CA");
      insert.value("locations",  locations);
      getSession().execute(insert);
   }
   
   public void createSchema2() {
      getSession().execute("CREATE KEYSPACE \"Simplex\" WITH " 
            + "replication = {'class':'SimpleStrategy', 'replication_factor':3};");
      getSession().execute("USE \"Simplex\";");
      getSession().execute("CREATE TABLE users (id uuid PRIMARY KEY, "
               + "name text, locations map<timestamp, text>);");
      Insert insert = QueryBuilder.insertInto("users");
      insert.value("id", UUID.randomUUID());
      insert.value("name", "Paul Tepley");
      Map<String, String> locations = new HashMap<String, String>();
      locations.put("2001-04-26 20:35", "San Diego, CA");
      locations.put("2009-02-11 09:29", "Boulder, CO");
      insert.value("locations", locations);
      getSession().execute(insert);
      insert = QueryBuilder.insertInto("users");
      insert.value("id", UUID.randomUUID());
      insert.value("name", "Tom Upton");
      locations = new HashMap<String, String>();
      locations.put("2000-12-24 12:32",  "Austin, TX");
      insert.value("locations",  locations);
      getSession().execute(insert);
      insert = QueryBuilder.insertInto("users");
      insert.value("id", UUID.randomUUID());
      insert.value("name", "Jim Bisso");
      locations = new HashMap<String, String>();
      locations.put("1993-03-01 00:00",  "Richmond, CA");
      insert.value("locations",  locations);
      getSession().execute(insert);
   }

   public void querySchema() {
      Statement statement = new SimpleStatement(
      		"INSERT INTO simplex.songs " +
      				"(id, title, album, artist) " +
      				"VALUES (da7c6910-a6a4-11e2-96a9-4db56cdc5fe7," +
      					"'Golden Brown', 'La Folie', 'The Stranglers'" +
      		");");
      try {
         getSession().execute(statement);
      } catch (NoHostAvailableException e) {
         System.out.printf("No host in the %s cluster can be contacted to execute the query.\n", 
         		getSession().getCluster());
      } catch (QueryExecutionException e) {
         System.out.println("An exception was thrown by Cassandra because it cannot " +
         		"successfully execute the query with the specified consistency level.");
      } catch (QueryValidationException e) {
         System.out.printf("The query %s \nis not valid, for example, incorrect syntax.\n",
         		statement.getQueryString());
      } catch (IllegalStateException e) {
         System.out.println("The BoundStatement is not ready.");
      }
   }
   
   public void updateSchema() {
      
   }

   public void dropSchema() {
      getSession().execute("DROP KEYSPACE \"Simplex\"");
      System.out.println("Finished dropping keyspace.");
   }

   public static void main(String[] args) {
      TestQueryBuilder client = new TestQueryBuilder();
      client.connect("127.0.0.1");
      client.createSchema();
      client.close();
   }
}

/*
CREATE TABLE users (id int PRIMARY KEY, name text, locations map<timestamp, text>);

*/
