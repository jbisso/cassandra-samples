require 'cassandra'

cluster   = Cassandra.connect()
session   = cluster.connect("simplex")
execution = session.execute("SELECT * FROM songs", :trace => true).execution_info
trace     = execution.trace

at_exit { cluster.close }

puts "coordinator: #{trace.coordinator}"
puts "started at: #{trace.started_at}"
puts "total events: #{trace.events.size}"
puts "request: #{trace.request}"
