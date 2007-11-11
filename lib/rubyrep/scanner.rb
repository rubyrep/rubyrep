module RR
  class Scanner

    attr_accessor :session, :left_table, :right_table

    # Creates a new Scanner instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      if session.left.primary_key_names(left_table).empty?
	raise "Table #{left_table} doesn't have a primary key. Cannot scan."
      end
      
      self.session, self.left_table, self.right_table = session, left_table, right_table
      self.right_table ||= self.left_table
    end

    # Runs the table scan.
    def run
    end
    
    # Generates the SQL query to iterate through the given target table.
    # Note: The column & order part of the query are always generated based on left_table.
    def construct_query(target_table)
      column_names = session.left.columns(left_table).map {|column| column.name}
      primary_key_names = session.left.primary_key_names left_table
      "select #{column_names.join(', ')} from #{target_table} order by #{primary_key_names.join(', ')}"
    end
  end
end
