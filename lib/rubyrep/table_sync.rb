module RR
  
  # Synchronizes the data of two tables.
  class TableSync < TableScan

    # Registers the specifies syncers.
    # +syncer_hash+ contains one or multiple syncers with
    #   * +:key+: identifier for the syncer
    #   * +:value+: the syncer class
    def self.register_syncer(syncer_hash)
      syncers.merge! syncer_hash
    end
    
    # Returns a hash of currently registered syncers. Construction of hash:
    #   * +:key+: identifier for the syncer
    #   * +:value+: the syncer class
    def self.syncers
      @@syncers ||= {}
      @@syncers
    end
    
    # Returns a hash of sync options for this table sync.
    def sync_options
      unless @sync_options
        options = session.configuration.sync_options
        if options[:table_specific]
          options[:table_specific].each do |table_hash|
            if table_hash.size > 1
              raise "table_specific sync options contains hashes with multiple entries" 
            end
            table_hash.each do |table, table_options|
              # note: using === as table might be a String or Regexp
              options.merge! table_options if table === left_table
            end
          end
        end
        options.delete :table_specific
        @sync_options = options
      end
      @sync_options      
    end
    
    # Creates a new TableSync instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      super
    end
    
    # Executes the table sync
    def run
      
    end
    
  end
end