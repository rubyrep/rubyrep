module RR

  # The Configuration class holds the default configuration options for Rubyrep.
  # Configuration values are changed with the Initializer::run method.
  class Configuration
    # Connection settings for the "left" database.
    # See Configuration#right for details.
    attr_accessor :left

    # Connection settings for the "right" database.
    # Takes a similar hash as ActiveRecord::Base.establish_connection.
    # Additional settings in case a proxy is used:
    # * :+proxy_host+: name or IP address of where the proxy is running
    # * :+proxy_port+: port on which the proxy is listening
    # Other additional settings:
    # * :+logger+:
    #   Specify an SQL statement logger for this database connection.
    #   Can be either
    #   * a logger instance itself (Logger or Log4r::Logger) or
    #   * the parameter to create a Logger with Logger.new
    #   Examples:
    #     config.left[:logger] = STDOUT
    #     config.right[:logger] = Logger.new('rubyrep_debug.log')
    attr_accessor :right
    
    # Returns true unless running on windows...
    def self.true_if_running_in_a_terminal_and_not_under_windows
      # Not using RUBY_PLATFORM as it should also work under JRuby
      $stdout.tty? and not ENV['OS'] =~ /windows/i
    end

    # Default #options for a new Configuration object.
    DEFAULT_OPTIONS = {
      :proxy_block_size => 1000,
      :row_buffer_size => 1000,
      :replicator => :two_way,
      :committer => :buffered_commit,
      :commit_frequency => 1000,
      :table_ordering => true,
      :scan_progress_printer => :progress_bar,
      :use_ansi => true_if_running_in_a_terminal_and_not_under_windows,
      :initial_sync => true,
      :adjust_sequences => true,
      :sequence_adjustment_buffer => 0,
      :sequence_increment => 2,
      :left_sequence_offset => 0,
      :right_sequence_offset => 1,
      :replication_interval => 1,
      :auto_key_limit => 0,
      :database_connection_timeout => 5,

      :rep_prefix => 'rr',
      :key_sep => '|',
    }
    
    # General options.
    # Possible settings:
    # * :+proxy_block_size+: The proxy cursor will calculate the checksum for block_size number of records each.
    # * :+row_buffer_size+:
    #   The number of rows that is read into memory at once.
    #   Only needed for database drivers that don't stream results one-by-one to the client.
    # * :+committer+:
    #   A committer key as registered by Committers#register.
    #   Determines the transaction management to be used during the sync.
    # * :+commit_frequency+:
    #   Used by BufferedCommitter. Number of changes after which the open
    #   transactions should be committed and new transactions be started.
    # * :+table_ordering+:
    #   If true, sort tables before syncing as per foreign key dependencies.
    #   (Dependent tables are synced last to reduce risk of foreign key
    #   constraint violations.)
    # * :+scan_progress_printer+:
    #   The progress printer key as registered by ScanProgressPrinters#register.
    #   Determines how the scan progress is visualized.
    # * :+use_ansi+: Only use ANSI codes for text output if +true+.
    # * :+auto_key_limit+:
    #   If a table has no primary keys and no primary keys have been specified
    #   manually using the :+primary_key_names+ option, then this option can be
    #   activated to simply use all columns of the table as a big combined key.
    #   This option specifies up to how many columns a table may have in order
    #   to use them as one big, combined primary key.
    #   Typical use case: the database has a lot of tables to map many-to-many
    #   relationshipts and no combined primary key is set up for them.
    # Sync specific settings
    # * :+before_table_sync+:
    #   A hook that is executed before a table sync.
    #   Can be either
    #   * a String: executed as SQL command on both databases.
    #   * a Proc:
    #     Called once before the table sync.
    #     The Proc is called with one parameter: the current SyncHelper instance.
    #     Through the sync helper there is access to the name of the synced table,
    #     the current session, etc
    #     Example:
    #     lambda {|helper| $stderr.puts "Hook called for #{helper.left_table}."}
    # * :+after_table_sync+:
    #   Same as :+before_table_sync+ (but called after the sync is completed).
    # * :+syncer+:
    #   A syncer key as registered by TableSync#register_syncer.
    #   Determines which sync algorithm is used.
    # * further options as defined by each syncer
    # Replication specific settings:
    # * :+rep_prefix+: the prefix that is put in front of all created database objects
    # * :+key_sep+: which string separates columns in the key column of the change log table
    # * :+replicator+:
    #   Determines which replicator algorithm to use.
    #   For each replicator must also exist a corresponding +:syncer+. (It is
    #   used for the initial sync of a table.)
    #   If no +:syncer+ option is specified, than a syncer as named by this
    #   option is used.
    # * :+initial_sync+:
    #   If +true+, syncs a table when initializing replication.
    #   Disable with care!
    #   (I. e. ensure that the table(s) have indeed same data in both databases
    #   before starting replication.)
    # * :+adjust_sequences+:
    #   If +true+, adjust sequences to avoid number conflicts between left and
    #   right database during replication.
    # * :+sequence_adjustement_buffer+:
    #   When updating a sequence, this is the additional gap to avoid sequence
    #   conflicts to appear due to concurrent record insertions.
    # * :+sequence_increment+: new sequence value = last sequence value + this
    # * :+left_sequence_offset+, +right_sequence_offset+:
    #   Default sequence offset for the table in the according data base.
    #   E. g. with a +sequence_increment+ of 2, an offset of 0 will produce even,
    #   an offset of 1 will produce odd numbers.
    # * :+replication_interval+: time in seconds between replication runs
    # * :+database_connection_timeout+:
    #   Time in seconds after which database connections time out.
    # * :+:after_infrastructure_setup+:
    #   A Proc that is called after the replication infrastructure tables are
    #   set up. Useful to e. g. tweak the access settings for the table.
    #   The block is called with the current Session object.
    #   The block is called every time replication is started, even if the
    #   the infrastructure tables already existed.
    #
    # Example of an :+after_infrastructure_setup+ handler:
    #   lambda do |session|
    #     [:left, :right].each do |database|
    #       session.send(database).execute "GRANT SELECT, UPDATE, INSERT ON rr_pending_changes TO scott"
    #     end
    #   end
    attr_reader :options
    
    # Merges the specified +options+ hash into the existing options
    def options=(options)
      @options ||= {}
      @options = @options.merge! options
    end

    # Array of table specifications for tables that should be processed
    # Refer to #add_table_options for what constitutes a valid table specification.
    def included_table_specs
      @included_table_specs ||= []
    end

    # Array of table specifications for tables that should *not* be processed
    # Refer to #add_table_options for what constitutes a valid table specification.
    def excluded_table_specs
      @excluded_table_specs ||= []
    end
    
    # Ensures that rubyrep infrastructure tables are excluded
    def exclude_rubyrep_tables
      exclude_tables Regexp.new("^#{options[:rep_prefix]}_.*")
    end
    
    # A list of tables having table specific options that should be considered
    # during processing (scanned, synced, ...)
    # +tables_with_options+ is a 2 element array with
    # * first element: A +table_spec+ (either a table name or a regexp matching multiple tables)
    # * second element: The +options+ hash (detailed format described in #add_tables
    # Should only be accessed via #add_table_options and #options_for_table
    def tables_with_options
      @tables_with_options ||= []
    end

    # Adds the specified tables to the list of tables that should be processed.
    # If options are provided, store them for future processing.
    # Refer to #add_table_options for detailed description of parameters.
    def include_tables(table_spec, options = nil)
      included_table_specs << table_spec unless included_table_specs.include?(table_spec)
      add_table_options(table_spec, options) if options
    end
    alias_method :include_table, :include_tables

    # Excludes the specified table from the list of tables that should be
    # processed.
    # Refer to #add_table_options for detailed description of what constitutes a
    # valid table specification.
    def exclude_tables(table_spec)
      excluded_table_specs << table_spec unless excluded_table_specs.include?(table_spec)
    end
    alias_method :exclude_table, :exclude_tables
    
    # Adds the specified options for the provided +table_spec+.
    # A +table_spec+ can be either
    # * a table name or
    # * a table pair (e. g. "my_left_table, my_right_table")
    # * a regexp matching multiple tables.
    # +options+ is hash with possible generic values as described under #options.
    # Additional, exclusively table specific options:
    # * :+primary_key_names+: array of primary key names
    def add_table_options(table_spec, options)
      i = nil
      tables_with_options.each_with_index { |table_options, k|
        i = k if table_options[0] == table_spec
      }
      if i
        table_options = tables_with_options[i][1]
      else
        table_options = {}
        tables_with_options << [table_spec, table_options]
      end
      table_options.merge! options
    end
    alias_method :add_table_option, :add_table_options

    # Yields all table specs that have been set up with the given option
    # * +key+: the option key
    # Yields:
    # * +table_spec+: the table specification of the matching option (or nil if non-table specific setting)
    # * +option_value+: the option value for the specified +key+
    def each_matching_option(key)
      yield nil, options[key] if options.include?(key)
      tables_with_options.each do |table_options|
        yield table_options[0], table_options[1][key] if table_options[1].include? key
      end
    end
    
    # Returns an option hash for the given table.
    # Accumulates options for all matching table specs (most recently added options
    # overwrite according options added before).
    #
    # Also includes the general options as returned by #options.
    # (Table specific options overwrite the general options).
    # 
    # Possible option values are described under #add_tables.
    def options_for_table(table)
      resulting_options = options.clone
      tables_with_options.each do |table_options|
        match = false
        if table_options[0].kind_of? Regexp
          match = (table_options[0] =~ table)
        else
          match = (table_options[0].sub(/(^.*),.*/,'\1').strip == table)
        end
        resulting_options.merge! table_options[1] if match
      end

      # Merge the default syncer& replicator options in
      [
        Syncers.configured_syncer(resulting_options),
        Replicators.configured_replicator(resulting_options)
      ].each do |processor_class|
        if processor_class.respond_to? :default_options
          default_processor_options = processor_class.default_options.clone
        else
          default_processor_options = {}
        end
        resulting_options = default_processor_options.merge!(resulting_options)
      end

      resulting_options
    end

    # initialize configuration settings
    def initialize
      self.left = {}
      self.right = {}
      self.options = DEFAULT_OPTIONS.clone
    end
    
  end
end