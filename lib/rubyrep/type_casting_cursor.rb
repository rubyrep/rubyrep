module RR
  # Provides functionality to cast a query result value into the correct ruby type.
  # Requires originating table and column to be known.
  class TypeCastingCursor

    # Delegate the uninteresting methods to the original cursor
    def next?; org_cursor.next? end
    def clear; org_cursor.clear end
    
    # The original cursor object
    attr_accessor :org_cursor
    
    # A column_name => Column cache
    attr_accessor :columns
    
    # Creates a new TypeCastingCursor based on provided database connection and table name
    # for the provided database query cursor
    def initialize(connection, table, cursor)
      self.org_cursor = cursor
      self.columns = {}
      connection.columns(table).each {|c| columns[c.name] = c}
    end
    
    # Reads the next row from the original cursor and returns the row with the type casted row values.
    def next_row
      row = org_cursor.next_row
      row.each {|column, value| row[column] = columns[column].type_cast value}
      row
    end    
  end
end
