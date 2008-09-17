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
    # Returns an array of table name pairs in Hash form.
    # For example something like
    #   [{:left_table => 'my_table', :right_table => 'my_table_backup'}]
    # Takes care that a table is only returned once.
    def resolve(table_specs)
      table_pairs = []
      table_specs.each do |table_spec|
        
        # If it is a regexp, convert it in an according string
        table_spec = table_spec.inspect if table_spec.kind_of? Regexp

        case table_spec
        when /^\/.*\/$/ # matches e. g. '/^user/'
          table_spec = table_spec.sub(/^\/(.*)\/$/,'\1') # remove leading and trailing slash
          matching_tables = tables.grep(Regexp.new(table_spec, Regexp::IGNORECASE, 'U'))
          matching_tables.each do |table|
            table_pairs << {:left_table => table, :right_table => table}
          end
        when /.+,.+/ # matches e. g. 'users,users_backup'
          pair = table_spec.match(/(.*),(.*)/)[1..2].map { |str| str.strip }
          table_pairs << {:left_table  => pair[0], :right_table => pair[1]}
        else # everything else: just a normal table
          table_pairs << {:left_table => table_spec.strip, :right_table => table_spec.strip}
        end
      end
      remove_duplicate_table_pairs(table_pairs)
    end

    # Helper for #resolve
    # Takes given table_pairs and removes all duplicates.
    # Returns the result.
    # Both the given and the returned table_pairs is an array of hashes with
    # * :+left_table+: name of the left table
    # * :+right_table+: name of the corresponding right table
    def remove_duplicate_table_pairs(table_pairs)
      processed_left_tables = {}
      resulting_table_pairs = []
      table_pairs.each do |table_pair|
        unless processed_left_tables.include? table_pair[:left_table]
          resulting_table_pairs << table_pair
          processed_left_tables[table_pair[:left_table]] = true
        end
      end
      resulting_table_pairs
    end
    private :remove_duplicate_table_pairs
  end
end