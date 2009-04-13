module RR
  
  # Resolves table specifications as provided e. g. in the command line of rrscan
  class TableSpecResolver
    
    # The +Session+ instance from which the table specifications are resolved.
    attr_accessor :session

    # Returns the array of tables of the specified database. Caches the table array.
    # * database: either :+left+ or :+right+
    def tables(database)
      @table_cache ||= {}
      unless @table_cache[database]
        @table_cache[database] = session.send(database).tables
      end
      @table_cache[database]
    end

    # Creates a resolver that works based on the given +Session+ instance.
    def initialize(session)
      self.session = session
    end

    # Returns all those tables from the given table_pairs that do not exist.
    # * +table_pairs+: same as described at #table_pairs_without_excluded
    # 
    # Returns:
    # A hash with keys :+left+ and +:right+, with the value for each key being
    # an array of non-existing tables for the according database.
    # The keys only exist if there are according missing tables.
    def non_existing_tables(table_pairs)
      [:left, :right].inject({}) do |memo, database|
        found_tables = table_pairs.inject([]) do |phantom_tables, table_pair|
          phantom_tables << table_pair[database] unless tables(database).include?(table_pair[database])
          phantom_tables
        end
        memo[database] = found_tables unless found_tables.empty?
        memo
      end
    end
    
    # Resolves the given array of table specificifications.
    # Table specifications are either
    # * strings as produced by BaseRunner#get_options or
    # * actual regular expressions
    # If +excluded_table_specs+ is provided, removes all tables that match it
    # (even if otherwise matching +included_table_specs+).
    #
    # If +verify+ is +true+, raises an exception if any non-existing tables are
    # specified.
    # 
    # Returns an array of table name pairs in Hash form.
    # For example something like
    #   [{:left => 'my_table', :right => 'my_table_backup'}]
    # 
    # Takes care that a table is only returned once.
    def resolve(included_table_specs, excluded_table_specs = [], verify = true)
      table_pairs = expand_table_specs(included_table_specs, verify)
      table_pairs = table_pairs_without_duplicates(table_pairs)
      table_pairs = table_pairs_without_excluded(table_pairs, excluded_table_specs)

      if verify
        non_existing_tables = non_existing_tables(table_pairs)
        unless non_existing_tables.empty?
          raise "non-existing tables specified: #{non_existing_tables.inspect}"
        end
      end

      table_pairs
    end

    # Helper for #resolve
    # Expands table specifications into table pairs.
    # Parameters:
    # * +table_specs+:
    #   An array of table specifications as described under #resolve.
    # * +verify+:
    #   If +true+, table specs in regexp format only resolve if the table exists
    #   in left and right database.
    # Return value: refer to #resolve for a detailed description
    def expand_table_specs(table_specs, verify)
      table_pairs = []
      table_specs.each do |table_spec|

        # If it is a regexp, convert it in an according string
        table_spec = table_spec.inspect if table_spec.kind_of? Regexp

        case table_spec
        when /^\/.*\/$/ # matches e. g. '/^user/'
          table_spec = table_spec.sub(/^\/(.*)\/$/,'\1') # remove leading and trailing slash
          matching_tables = tables(:left).grep(Regexp.new(table_spec, Regexp::IGNORECASE, 'U'))
          matching_tables.each do |table|
            if !verify or tables(:right).include? table
              table_pairs << {:left => table, :right => table}
            end
          end
        when /.+,.+/ # matches e. g. 'users,users_backup'
          pair = table_spec.match(/(.*),(.*)/)[1..2].map { |str| str.strip }
          table_pairs << {:left  => pair[0], :right => pair[1]}
        else # everything else: just a normal table
          table_pairs << {:left => table_spec.strip, :right => table_spec.strip}
        end
      end
      table_pairs
    end
    private :expand_table_specs

    # Helper for #resolve
    # Takes given table_pairs and removes all tables that are excluded.
    # Returns the result.
    # Both the given and the returned table_pairs is an array of hashes with
    # * :+left+: name of the left table
    # * :+right+: name of the corresponding right table
    # +excluded_table_specs+ is the array of table specifications to be excluded.
    def table_pairs_without_excluded(table_pairs, excluded_table_specs)
      excluded_tables = expand_table_specs(excluded_table_specs, false).map do |table_pair|
        table_pair[:left]
      end
      table_pairs.select {|table_pair| not excluded_tables.include? table_pair[:left]}
    end
    private :table_pairs_without_excluded

    # Helper for #resolve
    # Takes given table_pairs and removes all duplicates.
    # Returns the result.
    # Both the given and the returned table_pairs is an array of hashes with
    # * :+left+: name of the left table
    # * :+right+: name of the corresponding right table
    def table_pairs_without_duplicates(table_pairs)
      processed_left_tables = {}
      resulting_table_pairs = []
      table_pairs.each do |table_pair|
        unless processed_left_tables.include? table_pair[:left]
          resulting_table_pairs << table_pair
          processed_left_tables[table_pair[:left]] = true
        end
      end
      resulting_table_pairs
    end
    private :table_pairs_without_duplicates
  end
end