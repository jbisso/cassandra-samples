package com.example.cassandra;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class AsynchronousExample extends SimpleClient {
	public AsynchronousExample() {
	}
	
	public ResultSetFuture getRows() {
		Query query = QueryBuilder.select().all().from("simplex", "songs");
		return getSession().executeAsync(query);
	}

	public static void main(String[] args) {
		AsynchronousExample client = new AsynchronousExample();
		client.connect("127.0.0.1");
		client.createSchema();
		client.loadData();
		ResultSetFuture results = client.getRows();
      for (Row row : results.getUninterruptibly()) {
      	System.out.printf( "%s: %s / %s\n",
      			row.getString("artist"), 
      			row.getString("title"), 
      			row.getString("album") );
      }
		client.dropSchema("simplex");
		client.close();
	}
}
