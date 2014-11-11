class IgnoringKeyspacePolicy
    class Plan
        def has_next?
            false
        end
        
        def next
            nil
        end
    end
    
    def initialize(keyspace_to_ignore, original_policy)
        @keyspace = keyspace_to_ignore
        @policy   = original_policy
    end
    
    def setup(cluster)
    end
    
    def plan(keyspace, statement, options)
        if @keyspace == keyspace
            Plan.new
        else
            @policy.plan(keyspace, statement, options)
        end
    end
    
    def distance(host)
        @policy.distance(host)
    end
    
    def host_found(host)
        @policy.host_found(host)
    end
    
    def host_lost(host)
        @policy.host_lost(host)
    end
    
    def host_up(host)
        @policy.host_up(host)
    end
    
    def host_down(host)
        @policy.host_down(host)
    end
end
