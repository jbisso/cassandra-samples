package com.example.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

public class RollYourOwnCluster {
   private Session session;
   private static PreparedStatement preparedSelect;

   public RollYourOwnCluster() {
      Cluster cluster = Cluster.builder()
            .addContactPoints("127.0.0.1", "127.0.0.2")
            .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
            .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
            .build();
      session = cluster.connect();
   }

   public Session getSession() {
      return session;
   }
   
   public void createSchema() {
      session.execute(
         "CREATE KEYSPACE lexicon WITH replication " + 
         "= {'class':'SimpleStrategy', 'replication_factor':3};");
      session.execute(
         "CREATE TABLE lexicon.concordance (" +
               "id uuid," + 
               "word text PRIMARY KEY, " +
               "contexts list<text>, " +
               "occurrences int )");
   }
   
   public void loadData(File file) {
      PreparedStatement preparedInsert = session.prepare(
            "INSERT INTO lexicon.concordance " +
            "(id, word, contexts, occurrences) " +
            "VALUES (?, ?, ?, ?);");
      try {
         BufferedReader in = new BufferedReader(new FileReader(file));
         BoundStatement boundInsert;
         Map<String, List<String>> entries  = new HashMap<String, List<String>>();
         String line = "";
         while ( (line = in.readLine() ) != null ) {
            for (String word : line.split("[ \t\n\r.,!?:;\"'()]")) {
               String lemma = word.toLowerCase();
               if ( entries.containsKey(lemma) ) {
                  entries.get(lemma).add(line);
               } else {
                  List<String> contexts = new ArrayList<String>();
                  contexts.add(line);
                  entries.put(lemma, contexts);
               }
            }
         }
         for (String entry : entries.keySet()) {
            boundInsert = new BoundStatement(preparedInsert);
            boundInsert.bind(UUID.randomUUID(), entry, entries.get(entry), entries.get(entry).size());
            try {
               session.execute(boundInsert);
            } catch (Exception e) {
               System.err.printf("Problem inserting data: %s\n", e.getMessage());
            }
         }
         in.close();
      } catch (IOException ioe) {
         ioe.printStackTrace();
      }
   }

   public void queryData(String word) {
      BoundStatement boundSelect = new BoundStatement(preparedSelect);
      boundSelect.bind(word);
      ResultSet results = session.execute(boundSelect);
      Row row = results.one();
      System.out.printf("Word: %s; occurrences: %d\n", word, row.getList("contexts", String.class).size());
      for (String context : row.getList("contexts", String.class)) {
         System.out.printf("%s\n", context);
      }
   }

   public void close() {
      session.getCluster().shutdown();
      session.shutdown();
   }

   public static void main(String[] args) {
      RollYourOwnCluster client = new RollYourOwnCluster();
      client.createSchema();
      preparedSelect = client.session.prepare(
            "SELECT * FROM lexicon.concordance WHERE word = ?;");
      File file = new File(System.getProperty("user.home")
            + "/Documents/houndBaskervilles.txt");
      client.loadData(file);
      client.queryData("holmes");
      client.close();
   }
}
