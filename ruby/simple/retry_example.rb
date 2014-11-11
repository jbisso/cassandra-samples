require 'cassandra'
require_relative 'retrying_at_a_given_consistency_policy'

cluster   = Cassandra.connect(retry_policy: RetryingAtAGivenConsistencyPolicy.new(:one))
session   = cluster.connect("simplex")
execution = session.execute("SELECT * FROM songs", consistency: :all).execution_info

puts "requested consistency: #{execution.options.consistency}"
puts "actual consistency: #{execution.consistency}"
puts "number of retries: #{execution.retries}"
