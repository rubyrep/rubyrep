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
    
    # Creates a new TableSync instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    #   * sync_options: a hash of sync options
    def initialize(session, left_table, right_table = nil)
      super
    end
    
    # Executes the table sync
    def run
      
    end
    
  end
end