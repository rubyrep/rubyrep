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
      @sync_options ||= session.configuration.options_for_table(left_table)[:sync_options]
    end
    
    # Creates a new TableSync instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      super
    end
    
    # Executes the table sync. If a block is given, yields each difference with
    # the following 2 parameters
    # * +:type+
    # * +:row+
    # Purpose: enable display of progress information.
    # See DirectTableScan#run for full description of yielded parameters.
    def run
      success = false
      helper = SyncHelper.new(self)
      scan_class = TableScanHelper.scan_class(session)
      scan = scan_class.new(session, left_table, right_table)
      syncer_class = Syncers.syncers[sync_options[:syncer]]
      syncer = syncer_class.new(helper)

      scan.run do |type, row|
        yield type, row if block_given? # To enable progress reporting
        syncer.sync_difference type, row
      end
      success = true # considered to be successful if we get till here
    ensure
      helper.finalize success
    end
    
  end
end