package com.example.cassandra;

import com.example.cassandra.SimpleClient;

public class ResetSchema extends SimpleClient {
	public ResetSchema() {
	}
	
	public void dropSchema() {
		getSession().execute("DROP KEYSPACE simplex");
		System.out.println("Finished dropping keyspace.");
	}
	
	public static void main(String[] args) {
		ResetSchema client = new ResetSchema();
		client.connect("127.0.0.1");
		client.dropSchema();
		client.close();
	}
}
