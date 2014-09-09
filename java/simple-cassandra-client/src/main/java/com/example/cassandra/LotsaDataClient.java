package com.example.cassandra;

import java.util.UUID;

import com.datastax.driver.core.PreparedStatement;

public class LotsaDataClient extends SimpleClient {
    private static String INSERT_RANDOM_DATA = "INSERT INTO big.one (id, verba) VALUES (?, ?);";
    
    private PreparedStatement insertRandomData;
    
    public LotsaDataClient() {
    }
    
    public void prepareStatements() {
        insertRandomData = getSession().prepare(INSERT_RANDOM_DATA);
    }
    
    public void createSchema() {
        getSession().execute(
              "CREATE KEYSPACE big WITH replication " + 
              "= {'class':'SimpleStrategy', 'replication_factor':3};");
        getSession().execute("CREATE TABLE big.one (id int PRIMARY KEY, verba text);");
    }
    
    public void loadData() {
        for (int index = 0; index < 50000; ++index) {
            getSession().execute(insertRandomData.bind(index, UUID.randomUUID().toString()));
        }
    }
    
    public static void main(String[] args) {
        LotsaDataClient client = new LotsaDataClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.prepareStatements();
        client.loadData();
        client.pause();
        client.dropSchema("big");
        client.close();
    }

}
