require 'cassandra'
require_relative 'ignoring_keyspace_policy'

policy  = IgnoringKeyspacePolicy.new('simplex', Cassandra::LoadBalancing::Policies::RoundRobin.new)
cluster = Cassandra.connect(load_balancing_policy: policy)
session = cluster.connect('simplex')

begin
    session.execute("SELECT * FROM songs")
    puts "failure"
rescue Cassandra::Errors::NoHostsAvailable
    puts "success"
end

