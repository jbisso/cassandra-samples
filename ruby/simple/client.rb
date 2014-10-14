# encoding: utf-8

require_relative 'cassandraExamples'

client = CassandraExamples::SimpleClient.new()
# client = CassandraExamples::BoundStatementsClient.new()
client.connect('127.0.0.1')
client.createSchema()
client.loadData()
client.querySchema()
client.updateSchema()
client.pause("Hit <CR> to continue.")
client.dropSchema("simplex")
client.close()

