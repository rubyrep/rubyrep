module RR
  
  # Synchronizes the data of two tables.
  class TableSync < TableScan

    # Instance of SyncHelper
    attr_accessor :helper

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

    # Executes the specified sync hook
    # * +hook_id+: either :+before_table_sync+ or :+after_table_sync+
    def execute_sync_hook(hook_id)
      hook = sync_options[hook_id]
      if hook
        if hook.respond_to?(:call)
          hook.call(helper)
        else
          [:left, :right].each do |database|
            session.send(database).execute hook
          end
        end
      end
    end

    # Calls the event filter for the give table difference.
    # * +type+: type of difference
    # * +row+: the differing row
    # Refer to DirectTableScan#run for full description of +type+ and +row+.
    # Returns +true+ if syncing of the difference should *not* proceed.
    def event_filtered?(type, row)
      event_filter = sync_options[:event_filter]
      if event_filter && event_filter.respond_to?(:before_sync)
        not event_filter.before_sync(
          helper.left_table,
          helper.extract_key([row].flatten.first),
          helper,
          type,
          row
        )
      else
        false
      end
    end

    # Executes the table sync. If a block is given, yields each difference with
    # the following 2 parameters
    # * +type+
    # * +row+
    # Purpose: enable display of progress information.
    # See DirectTableScan#run for full description of yielded parameters.
    def run
      success = false

      scan_class = TableScanHelper.scan_class(session)
      scan = scan_class.new(session, left_table, right_table)
      scan.progress_printer = progress_printer

      self.helper = SyncHelper.new(self)
      syncer = Syncers.configured_syncer(sync_options).new(helper)
    
      execute_sync_hook :before_table_sync

      scan.run do |type, row|
        yield type, row if block_given? # To enable progress reporting
        unless event_filtered?(type, row)
          syncer.sync_difference type, row
        end
      end
      
      execute_sync_hook :after_table_sync

      success = true # considered to be successful if we get till here
    ensure
      helper.finalize success if helper
    end
    
  end
end