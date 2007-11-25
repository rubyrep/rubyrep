module RR
  
  # Scans two tables for differences. Goes through a RubyRep Proxy to minimize network load.
  # Doesn't have any reporting functionality by itself. 
  # Instead ProxiedTableScan#run yields all the differences for the caller to do with as it pleases.
  # Usage:
  #   1. Create a new ProxiedTableScan object and hand it all necessary information
  #   2. Call ProxiedTableScan#run to do the actual comparison
  #   3. The block handed to ProxiedTableScan#run receives all differences
  class ProxiedTableScan

    attr_accessor :session, :left_table, :right_table

    # Cached array of primary key names
    attr_accessor :primary_key_names

    # Creates a new ProxiedTableScan instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      
      if session.left.primary_key_names(left_table).empty?
        raise "Table #{left_table} doesn't have a primary key. Cannot scan."
      end
      
      self.session, self.left_table, self.right_table = session, left_table, right_table
      self.right_table ||= self.left_table
      self.primary_key_names = session.left.primary_key_names left_table
    end

  
  end
end