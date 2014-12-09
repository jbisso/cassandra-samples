require 'cassandra'
require_relative 'membership_change_printing_listener'

interval = 2 # reconnect every 2 seconds
policy   = Cassandra::Reconnection::Policies::Constant.new(interval)
cluster  = Cassandra.connect(
        listeners: [MembershipChangePrintingListener.new($stdout)],
        reconnection_policy: policy,
        consistency: :one
        )
session = cluster.connect

$stdout.puts("=== START ===")
$stdout.flush
until (input = $stdin.gets).nil? # block until closed
    query = input.chomp
    begin
        execution_info = session.execute(query).execution_info
        $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts.last.ip}")
    rescue => e
        $stdout.puts("Query #{query.inspect} failed with #{e.class.name}: #{e.message}")
    end
    $stdout.flush
end
$stdout.puts("=== STOP ===")
$stdout.flush

