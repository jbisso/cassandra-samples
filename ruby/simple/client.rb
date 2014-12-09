# encoding: utf-8

require_relative 'cassandraExamples'

client = CassandraExamples::SimpleClient.new
# client = CassandraExamples::BoundStatementsClient.new
client.connect(['127.0.0.1'])
client.create_schema
client.load_data
client.query_schema
client.update_schema
client.pause("Hit <CR> to continue.")
client.drop_schema("simplex")
client.close

