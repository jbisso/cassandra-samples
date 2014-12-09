class SchemaChangePrintingListener
    def initialize(io)
        @out = io
    end
    
    def keyspace_created(keyspace)
        @out.puts("Keyspace #{keyspace.name.inspect} created")
    end
    
    def keyspace_changed(keyspace)
        @out.puts("Keyspace #{keyspace.name.inspect} changed")
    end
    
    def keyspace_dropped(keyspace)
        @out.puts("Keyspace #{keyspace.name.inspect} dropped")
    end
end
