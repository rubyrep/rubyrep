require 'drb'

module RR

  # This class represents a rubyrep session.
  # Creates and holds expensive objects like e. g. database connections.
  class Session
    
    # The Configuration object provided to the initializer
    attr_accessor :configuration
    
    # Returns the "left" ActiveRecord / proxy database connection
    def left
      @connections[:left]
    end
    
    # Stores the "left" ActiveRecord /proxy database connection
    def left=(connection)
      @connections[:left] = connection
    end
    
    # Returns the "right" ActiveRecord / proxy database connection
    def right
      @connections[:right]
    end
    
    # Stores the "right" ActiveRecord / proxy database connection
    def right=(connection)
      @connections[:right] = connection
    end
    
    # Hash to hold under either :left or :right the according Drb / direct DatabaseProxy
    attr_accessor :proxies

    # Creates a hash of manual primary key names as can be specified with the
    # Configuration options :+primary_key_names+ or :+auto_key_limit+.
    # * +db_arm: should be either :left or :right
    #
    # Returns the identified manual primary keys. This is a hash with
    # * key: table_name
    # * value: array of primary key names
    def manual_primary_keys(db_arm)
      manual_primary_keys = {}
      resolver = TableSpecResolver.new self
      table_pairs = resolver.resolve configuration.included_table_specs, [], false
      table_pairs.each do |table_pair|
        options = configuration.options_for_table(table_pair[:left])
        key_names = options[:key]
        if key_names == nil and options[:auto_key_limit] > 0
          if left.primary_key_names(table_pair[:left], :raw => true).empty?
            column_names = left.column_names(table_pair[:left])
            if column_names.size <= options[:auto_key_limit]
              key_names = column_names
            end
          end
        end
        if key_names
          table_name = table_pair[db_arm]
          manual_primary_keys[table_name] = [key_names].flatten
        end
      end
      manual_primary_keys
    end

    # Returns the corresponding table in the other database.
    # * +db_arm+: database of the given table (either :+left+ or :+right+)
    # * +table+: name of the table
    #
    # If no corresponding table can be found, return the given table.
    # Rationale:
    # Support the case where a table was dropped from the configuration but
    # there were still some unreplicated changes left.
    def corresponding_table(db_arm, table)
      unless @table_map
        @table_map = {:left => {}, :right => {}}
        resolver = TableSpecResolver.new self
        table_pairs = resolver.resolve configuration.included_table_specs, [], false
        table_pairs.each do |table_pair|
          @table_map[:left][table_pair[:left]] = table_pair[:right]
          @table_map[:right][table_pair[:right]] = table_pair[:left]
        end
      end
      @table_map[db_arm][table] || table
    end
    
    # Returns +true+ if proxy connections are used
    def proxied?
      [configuration.left, configuration.right].any? \
        {|arm_config| arm_config.include? :proxy_host}
    end

    # Returns an array of table pairs of the configured tables.
    # Refer to TableSpecResolver#resolve for a detailed description of the
    # return value.
    # If +included_table_specs+ is provided (that is: not an empty array), it
    # will be used instead of the configured table specs.
    def configured_table_pairs(included_table_specs = [])
      resolver = TableSpecResolver.new self
      included_table_specs = configuration.included_table_specs if included_table_specs.empty?
      resolver.resolve included_table_specs, configuration.excluded_table_specs
    end

    # Orders the array of table pairs as per primary key / foreign key relations
    # of the tables. Returns the result.
    # Only sorts if the configuration has set option :+table_ordering+.
    # Refer to TableSpecResolver#resolve for a detailed description of the
    # parameter and return value.
    def sort_table_pairs(table_pairs)
      if configuration.options[:table_ordering]
        left_tables = table_pairs.map {|table_pair| table_pair[:left]}
        sorted_left_tables = TableSorter.new(self, left_tables).sort
        sorted_left_tables.map do |left_table|
          table_pairs.find do |table_pair|
            table_pair[:left] == left_table
          end
        end
      else
        table_pairs
      end
    end

    # Returns +true+ if the specified database connection is not alive.
    # * +database+: target database (either +:left+ or :+right+)
    def database_unreachable?(database)
      unreachable = true
      Thread.new do
        begin
          if send(database) && send(database).select_one("select 1+1 as x")['x'].to_i == 2
            unreachable = false # database is actually reachable
          end
        end rescue nil
      end.join configuration.options[:database_connection_timeout]
      unreachable
    end

    # Disconnects both database connections
    def disconnect_databases
      [:left, :right].each do |database|
        disconnect_database(database)
      end
    end

    # Disconnnects the specified database
    # * +database+: the target database (either :+left+ or :+right+)
    def disconnect_database(database)
      proxy, connection = @proxies[database], @connections[database]
      @proxies[database] = nil
      @connections[database] = nil
      if proxy
        proxy.destroy_session(connection)
      end
    end

    # Refreshes both database connections
    # * +options+: A options hash with the following settings
    #   * :+forced+: if +true+, always establish a new database connection
    def refresh(options = {})
      [:left, :right].each {|database| refresh_database_connection database, options}
    end

    # Refreshes the specified database connection.
    # (I. e. reestablish if not active anymore.)
    # * +database+: target database (either :+left+ or :+right+)
    # * +options+: A options hash with the following settings
    #   * :+forced+: if +true+, always establish a new database connection
    def refresh_database_connection(database, options)
      if options[:forced] or database_unreachable?(database)
        # step 1: disconnect both database connection (if still possible)
        begin
          Thread.new do
            disconnect_database database rescue nil
          end.join configuration.options[:database_connection_timeout]
        end

        connect_exception = nil
        # step 2: try to reconnect the database
        Thread.new do
          begin
            connect_database database
          rescue Exception => e
            # save exception so it can be rethrown outside of the thread
            connect_exception = e
          end
        end.join configuration.options[:database_connection_timeout]
        raise connect_exception if connect_exception

        # step 3: verify if database connections actually work (to detect silent connection failures)
        if database_unreachable?(database)
          raise "no connection to '#{database}' database"
        end
      end
    end

    # Set up the (proxied or direct) database connections to the specified
    # database.
    # * +database+: the target database (either :+left+ or :+right+)
    def connect_database(database)
      if configuration.left == configuration.right and database == :right
        # If both database configurations point to the same database
        # then don't create the database connection twice.
        # Assumes that the left database is always connected before the right one.
        self.right = self.left
      else
        # Connect the database / proxy
        arm_config = configuration.send database
        if arm_config.include? :proxy_host
          drb_url = "druby://#{arm_config[:proxy_host]}:#{arm_config[:proxy_port]}"
          @proxies[database] = DRbObject.new nil, drb_url
        else
          # Create fake proxy
          @proxies[database] = DatabaseProxy.new
        end
        @connections[database] = @proxies[database].create_session arm_config

        send(database).manual_primary_keys = manual_primary_keys(database)
      end
    end
        
    # Creates a new rubyrep session with the provided Configuration
    def initialize(config = Initializer::configuration)
      @connections = {:left => nil, :right => nil}
      @proxies = {:left => nil, :right => nil}
      
      # Keep the database configuration for future reference
      self.configuration = config

      refresh
    end
  end
end
