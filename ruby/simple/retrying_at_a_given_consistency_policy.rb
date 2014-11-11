class RetryingAtAGivenConsistencyPolicy
    include Cassandra::Retry::Policy
    
    def initialize(consistency_to_use)
        @consistency_to_use = consistency_to_use
    end
    
    def read_timeout(statement, consistency_level, required_responses,
                   received_responses, data_retrieved, retries)
        try_again(@consistency_to_use)
    end
    
    def write_timeout(statement, consistency_level, write_type,
                    acks_required, acks_received, retries)
        try_again(@consistency_to_use)
    end
    
    def unavailable(statement, consistency_level, replicas_required,
                  replicas_alive, retries)
        try_again(@consistency_to_use)
    end
end
