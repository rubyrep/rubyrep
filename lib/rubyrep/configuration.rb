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
    # * +proxy_host+: name or IP address of where the proxy is running
    # * +proxy_port+: port on which the proxy is listening
    attr_accessor :right
    
    # Returns true unless running on windows...
    def self.true_unless_running_on_windows
      # Not using RUBY_PLATFORM as it should also work under JRuby
      not ENV['OS'] =~ /windows/i
    end

    # Default #options for a new Configuration object.
    DEFAULT_OPTIONS = {
      :proxy_block_size => 1000,
      :syncer => :two_way,
      :committer => :default,
      :table_ordering => true,
      :scan_progress_printer => :progress_bar,
      :use_ansi => true_unless_running_on_windows,

      :rep_prefix => 'rr',
      :key_sep => '|',
      :sequence_adjustment_buffer => 10,
    }
    
    # General options.
    # Possible settings:
    # * :+proxy_block_size+: The proxy cursor will calculate the checksum for block_size number of records each.
    # * :+committer+:
    #   A committer key as registered by Committers#register.
    #   Determines the transaction management to be used during the sync.
    # * :+table_ordering+:
    #   If true, sort tables before syncing as per foreign key dependencies.
    #   (Dependent tables are synced last to reduce risk of foreign key
    #   constraint violations.)
    # * :+scan_progress_printer+:
    #   The progress printer key as registered by ScanProgressPrinters#register.
    #   Determines how the scan progress is visualized.
    # * :+use_ansi+: Only use ANSI codes for text output if +true+.
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
    # *:+sequence_adjustement_buffer+:
    #   When updating a sequence, this is the additional gap to avoid sequence
    #   conflicts to appear due to concurrent record insertions.
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

    # Excludes the specified table from the list of tables that should be
    # processed.
    # Refer to #add_table_options for detailed description of what constitutes a
    # valid table specification.
    def exclude_tables(table_spec)
      excluded_table_specs << table_spec unless excluded_table_specs.include?(table_spec)
    end
    
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

      # Merge the default syncer options in (if syncer has some)
      syncer_class = Syncers.syncers[resulting_options[:syncer]]
      if syncer_class.respond_to? :default_options
        default_syncer_options = syncer_class.default_options.clone
      else
        default_syncer_options = {}
      end
      resulting_options = default_syncer_options.merge! resulting_options

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