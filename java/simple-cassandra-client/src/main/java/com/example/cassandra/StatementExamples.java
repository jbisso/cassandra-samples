package com.example.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class StatementExamples extends MetricsExample {
    public StatementExamples() {
    }
    
    @Override
    public void querySchema() {
        System.out.println("Data queried.");
        Statement statement01 = new SimpleStatement("SELECT * FROM lexicon.concordance WHERE word = 'revolver';");
        System.out.printf( "Default consistency level = %s\n",  statement01.getConsistencyLevel() );
        statement01.setConsistencyLevel(ConsistencyLevel.ONE);
        System.out.printf( "New consistency level = %s\n",  statement01.getConsistencyLevel() );
        ResultSet results = getSession().execute(statement01);
        for ( Row row : results ) {
            System.out.printf( "%s: %d\n",  row.getString("word"), row.getInt("occurrences"));
        }
        // Fetch size
        Statement statement02 = new SimpleStatement("SELECT * FROM lexicon.concordance;");
        statement02.setFetchSize(100);
        results = getSession().execute(statement02);
        int numberRowsFetched = results.getAvailableWithoutFetching();
        for ( Row row : results ) {
            System.out.printf( "%2d %s: %d\n", numberRowsFetched, row.getString("word"), row.getInt("occurrences"));
        }
    }

    public static void main(String[] args) {
        StatementExamples client = new StatementExamples();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.querySchema();
        client.dropSchema("lexicon");
        client.close();
    }

}

                