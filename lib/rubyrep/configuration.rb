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
    
    # Default #proxy_options for a new Configuration object.
    DEFAULT_PROXY_OPTIONS = {
      :block_size => 1000
    }
    
    # Default options for a table sync
    DEFAULT_SYNC_OPTIONS = {
      :syncer => :two_way,
      :committer => :default
    }
    
    # General options for the proxy operation mode.
    # Possible settings:
    # * :+block_size+: To proxy cursor will calculate the checksum for block_size number of records each.
    attr_reader :proxy_options
    
    # Set the specified +options+ hash as new proxy options +after+ merging them
    # into the default proxy option hash.
    def proxy_options=(options)
      @proxy_options = DEFAULT_PROXY_OPTIONS.clone.merge!(options)
    end
    
    # Table sync options. A hash with the following possible settings:
    # * :+committer+:
    #   A committer key as registered by Committers#register.
    #   Determines the transaction management to be used during the sync.
    # * :+syncer+:
    #   A syncer key as registered by TableSync#register_syncer.
    #   Determines which sync algorithm is used.
    # * further options as defined by each syncer
    # * :+table_specific+: An array of table specific options.
    #   Each array element consists of a 1 entry hash with
    #   * key: A table name string or a Regexp matching multiple tables.
    #   * values: An hash with sync options as described abobve.
    attr_reader :sync_options
    
    # Set the specified +options+ hash as new sync options +after+ merging them
    # into the default proxy option hash.
    def sync_options=(options)
      @sync_options = DEFAULT_SYNC_OPTIONS.clone.merge!(options)
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
    
    # Adds the specified +table_spec+ and it's options (if provided).
    # A +table_spec+ can be either a table name or a regexp matching multiple tables.
    # +options+ is a multi-element hash with
    # * key: Designates type of options. Either :proxy_options or :sync_options
    # * values: The according table specific options as described under #proxy_options or #sync_options
    def add_tables(table_spec, options = {})
      i = nil
      tables_with_options.each_with_index { |table_options, k|
        i = k if table_options[0] == table_spec
      }
      if i
        table_options = tables_with_options[i][1]
      else
        table_options = {:proxy_options => {}, :sync_options => {}}
        tables_with_options << [table_spec, table_options]
      end
      table_options[:proxy_options].merge! options[:proxy_options] || {}
      table_options[:sync_options].merge! options[:sync_options] || {}
    end
    
    # Returns an option hash for the given table.
    # Accumulates options for all matching table specs (most recently added options
    # overwrite according options added before).
    # Refer to #add_tables for the exact format of the returned options.
    #
    # Note:
    # The returned sync options also include the default options of the
    # syncer specified by the :syncer option.
    # (Specifically provided options overwrite the default sync options.)
    def options_for_table(table)
      table_proxy_options = proxy_options.clone
      table_sync_options = sync_options.clone
      tables_with_options.each do |table_options|
        if table_options[0] === table
          table_proxy_options.merge! table_options[1][:proxy_options]
          table_sync_options.merge! table_options[1][:sync_options]
        end
      end

      # Merge the default syncer options in (if syncer has some)
      syncer_class = Syncers.syncers[table_sync_options[:syncer]]
      if syncer_class.respond_to? :default_options
        default_syncer_options = syncer_class.default_options.clone
      else
        default_syncer_options = {}
      end
      table_sync_options = default_syncer_options.merge! table_sync_options

      {:proxy_options => table_proxy_options, :sync_options => table_sync_options}
    end
    
    # initialize attributes with empty hashes
    def initialize
      [:left, :right].each do |hash_attr|
        eval "self.#{hash_attr}= {}"
      end
      self.proxy_options = DEFAULT_PROXY_OPTIONS.clone
      self.sync_options = DEFAULT_SYNC_OPTIONS.clone
    end
    
  end
end