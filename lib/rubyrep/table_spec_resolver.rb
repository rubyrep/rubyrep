module RR
  
  # Resolves table specification springs in actual table name pairs.
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
    
    # Resolves the given array of table specificification strings (as produced by
    # ScanRunner#get_options). Returns an array of table name pairs in Hash form. 
    # E. g. something like 
    # [{:left_table => 'my_table', :right_table => 'my_table_backup'}]
    def resolve(table_specs)
      table_pairs = []
      table_specs.each do |table_spec|
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
      table_pairs
    end
  end
end