$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

module RR
  # Fakes the interface of a database proxy.
  # Reason for doing that: ProxyScanner works with both or either of the two
  # database connections being proxied.
  # Using FakeDatabaseProxy, the ProxyScanner algorithm can always assume that
  # both database connection are proxied.
  class FakeDatabaseProxy
    
    def initialize
      
    end
  end
end
