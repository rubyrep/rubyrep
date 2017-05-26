module RR
  # Provides functionality to cast a query result value into the correct ruby type.
  # Requires originating table and column to be known.
  class TypeCastingCursor

    # Delegate the uninteresting methods to the original cursor
    def next?; org_cursor.next? end
    def clear; org_cursor.clear end
    def connection; org_cursor.connection end
    def options; org_cursor.options end

    # @return [ResultFetcher] the original cursor
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
      row.each do |column, value|
        row[column] = connection.connection.fixed_type_cast value, columns[column]
      end
      row
    end    
  end
end
