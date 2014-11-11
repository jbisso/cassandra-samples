require 'cassandra'

cluster = Cassandra.connect(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
session = cluster.connect('simplex')

begin
  session.execute('SELECT * FROM songs', consistency: :all)
  puts "failed"
rescue Cassandra::Errors::UnavailableError => e
  puts "success"
end
