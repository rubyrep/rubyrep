module RR
  class Scanner

    attr_accessor :session, :left_table, :right_table

    def initialize(session, left_table, right_table = nil)
      if session.left.primary_key_names(left_table).empty?
	raise "Table #{left_table} doesn't have a primary key. Cannot scan."
      end
      
      self.session, self.left_table, self.right_table = session, left_table, right_table
      self.right_table ||= self.left_table
    end

    def run
    end

    def construct_query
      column_names = session.left.columns(left_table).map {|column| column.name}
      primary_key_names = session.left.primary_key_names left_table
      "select #{column_names.join(', ')} from #{left_table} order by #{primary_key_names.join(', ')}"
    end
  end
end
