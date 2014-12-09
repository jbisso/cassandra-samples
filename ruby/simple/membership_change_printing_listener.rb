class MembershipChangePrintingListener
    def initialize(io)
        @out = io
    end
    
    def host_found(host)
        @out.puts("Host #{host.ip} is found")
        @out.flush
    end
    
    def host_lost(host)
        @out.puts("Host #{host.ip} is lost")
        @out.flush
    end
    
    def host_up(host)
        @out.puts("Host #{host.ip} is up")
        @out.flush
    end
    
    def host_down(host)
        @out.puts("Host #{host.ip} is down")
        @out.flush
    end
end
