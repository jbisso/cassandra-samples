require 'cassandra'

cluster = Cassandra.connect(retry_policy: Cassandra::Retry::Policies::DowngradingConsistency.new)
session = cluster.connect('simplex')
result  = session.execute('SELECT * FROM songs', consistency: :all)

puts "actual consistency: #{result.execution_info.consistency}"
