module RR
  
  # Synchronizes the data of two tables.
  class TableSync < TableScan

    # Returns a hash of sync options for this table sync.
    def sync_options
      @sync_options ||= session.configuration.options_for_table(left_table)
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
      helper = nil

      scan_class = TableScanHelper.scan_class(session)
      scan = scan_class.new(session, left_table, right_table)
      scan.progress_printer = progress_printer

      helper = SyncHelper.new(self)
      syncer = Syncers.configured_syncer(sync_options).new(helper)

      scan.run do |type, row|
        yield type, row if block_given? # To enable progress reporting
        syncer.sync_difference type, row
      end
      success = true # considered to be successful if we get till here
    ensure
      helper.finalize success if helper
    end
    
  end
end