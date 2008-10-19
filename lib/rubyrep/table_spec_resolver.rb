module RR
  
  # Resolves table specifications as provided e. g. in the command line of rrscan
  class TableSpecResolver
    
    # The +Session+ instance from which the table specifications are resolved.
    attr_accessor :session
    
    # Caches the table name array returned by 'left' database session
    attr_accessor :tables
    
    # Creates a resolver that works based on the given +Session+ instance.
    def initialize(session)
      self.session = session
      self.tables = session.left.tables
    end
    
    # Resolves the given array of table specificifications.
    # Table specifications are either
    # * strings as produced by BaseRunner#get_options or
    # * actual regular expressions
    # If +excluded_table_specs+ is provided, removes all tables that match it
    # (even if otherwise matching +included_table_specs+).
    # Returns an array of table name pairs in Hash form.
    # For example something like
    #   [{:left => 'my_table', :right => 'my_table_backup'}]
    # Takes care that a table is only returned once.
    def resolve(included_table_specs, excluded_table_specs = [])
      table_pairs = expand_table_specs(included_table_specs)
      table_pairs = table_pairs_without_duplicates(table_pairs)
      table_pairs_without_excluded(table_pairs, excluded_table_specs)
    end

    # Helper for #resolve
    # Takes the specified table_specifications and expands it into an array of
    # according table pairs.
    # Returns the result
    # Refer to #resolve for a full description of parameters and result.
    def expand_table_specs(table_specs)
      table_pairs = []
      table_specs.each do |table_spec|

        # If it is a regexp, convert it in an according string
        table_spec = table_spec.inspect if table_spec.kind_of? Regexp

        case table_spec
        when /^\/.*\/$/ # matches e. g. '/^user/'
          table_spec = table_spec.sub(/^\/(.*)\/$/,'\1') # remove leading and trailing slash
          matching_tables = tables.grep(Regexp.new(table_spec, Regexp::IGNORECASE, 'U'))
          matching_tables.each do |table|
            table_pairs << {:left => table, :right => table}
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
      excluded_tables = expand_table_specs(excluded_table_specs).map do |table_pair|
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