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
    #   * +proxy_host+: name or IP address of where the proxy is running
    #   * +proxy_port+: port on which the proxy is listening
    attr_accessor :right
    
    # Default #proxy_options for a new Configuration object.
    DEFAULT_PROXY_OPTIONS = {
      :block_size => 1000
    }
    
    # Default options for a table sync
    DEFAULT_SYNC_OPTIONS = {
      :syncer => :two_way
    }
    
    # General options for the proxy operation mode.
    # Possible settings:
    #   * +:block_size+: To proxy cursor will calculate the checksum for block_size number of records each.
    attr_accessor :proxy_options
    
    # Table sync options. A hash with the following possible settings:
    # * +:syncer+: A syncer key as registered by TableSync#register_syncer
    # * further options as defined by each syncer
    # * +:table_specific+: An array of table specific options.
    #   Each array element consists of a 1 entry hash with
    #   * key: A table name string or a Regexp matching multiple tables.
    #   * values: An hash with sync options as described abobve.
    attr_accessor :sync_options
    
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