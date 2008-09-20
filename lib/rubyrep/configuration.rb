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
    
    # Default #options for a new Configuration object.
    DEFAULT_OPTIONS = {
      :proxy_block_size => 1000,
      :syncer => :two_way,
      :committer => :default,
      :table_ordering => true,
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
    # * :+syncer+:
    #   A syncer key as registered by TableSync#register_syncer.
    #   Determines which sync algorithm is used.
    # * further options as defined by each syncer
    attr_reader :options
    
    # Merges the specified +options+ hash into the existing options
    def options=(options)
      @options ||= {}
      @options = @options.merge! options
    end
    
    # A list of tables that should be processed (scanned, synced, ...) togehter
    # with the table specific options.
    # +tables_with_options+ is a 2 element array with
    # * first element: A +table_spec+ (either a table name or a regexp matching multiple tables)
    # * second element: The +options+ hash (detailed format described in #add_tables
    # Should only be accessed via #add_tables and #options_for_table
    def tables_with_options
      @tables_with_options ||= []
    end

    # Returns an array containing the configured table specifications.
    # (#add_tables describes the format of valid table specifications.)
    def tables
      tables_with_options.map {|table_options| table_options[0]}
    end
    
    # Adds the specified +table_spec+ and it's options (if provided).
    # A +table_spec+ can be either
    # * a table name or
    # * a table pair (e. g. "my_left_table, my_right_table")
    # * a regexp matching multiple tables.
    # +options+ is hash with possible values as described under #options.
    def add_tables(table_spec, options = {})
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
    # Possible option values are described under #options.
    def options_for_table(table)
      resulting_options = options.clone
      tables_with_options.each do |table_options|
        resulting_options.merge! table_options[1] if table_options[0] === table
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