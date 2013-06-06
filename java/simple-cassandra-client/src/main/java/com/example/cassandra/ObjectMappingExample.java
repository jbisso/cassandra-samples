package com.example.cassandra;

public class ObjectMappingExample extends SimpleClient {
	public ObjectMappingExample() {
	}

	public static void main(String[] args) {
		ObjectMappingExample client = new ObjectMappingExample();
		client.connect("127.0.0.1");
		client.createSchema();
		client.loadData();
		
		client.dropSchema("simplex");
		client.close();
	}

}
