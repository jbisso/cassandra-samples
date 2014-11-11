require 'cassandra'

cluster   = Cassandra.connect()
session   = cluster.connect("simplex")
execution = session.execute("SELECT * FROM songs").execution_info

at_exit { cluster.close }

if execution.trace
    puts "Failure."
else
    puts "Success."
end
