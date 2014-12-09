require 'cassandra'
require_relative 'schema_change_printing_listener'

listener = SchemaChangePrintingListener.new($stderr)
cluster  = Cassandra.connect(hosts: ['127.0.0.1'])

cluster.register(listener)

$stdout.puts("=== START ===")
$stdout.flush
$stdin.gets
$stdout.puts("=== STOP ===")
$stdout.flush
