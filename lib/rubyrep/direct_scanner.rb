module RR

  # Scans two tables for differences.
  # Doesn't have any reporting functionality by itself. 
  # Instead DirectScanner#run yields all the differences for the caller to do with as it pleases.
  # Usage:
  #   1. Create a new DirectScanner object and hand it all necessary information
  #   2. Call DirectScanner#run to do the actual comparison
  #   3. The block handed to DirectScanner#run receives all differences
  class DirectScanner

    attr_accessor :session, :left_table, :right_table

    # Cached array of primary key names
    attr_accessor :primary_key_names

    # Creates a new DirectScanner instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      if session.left.primary_key_names(left_table).empty?
        raise "Table #{left_table} doesn't have a primary key. Cannot scan."
      end
      
      self.session, self.left_table, self.right_table = session, left_table, right_table
      self.right_table ||= self.left_table
      self.primary_key_names = session.left.primary_key_names left_table
    end

    # Runs the table scan.
    # Calls the block for every found difference.
    # Differences are yielded with 2 parameters
    #   * type: describes the difference, either :left (row only in left table), :right (row only in right table) or :conflict
    #   * row: for :left or :right cases a hash describing the row; for :conflict an array of left and right row
    def run(&blck)
      left_cursor = right_cursor = nil
      begin
        left_cursor = session.left.select_cursor construct_query(left_table)
        right_cursor = session.right.select_cursor construct_query(right_table) 
        left_row = right_row = nil
        while left_cursor.next?
          # if there is no current left row, load the next one
          left_row ||= left_cursor.next_row
          # if there is no current right row, _try_ to load the next one
          right_row ||= right_cursor.next_row if right_cursor.next?
          if right_row == nil
            # no more rows in right, all remaining left rows exist only there
            # yield the current unprocessed left row
            yield :left, left_row
            left_row = nil
            while left_cursor.next?
              # yield all remaining left rows
              yield :left, left_cursor.next_row
            end
            break
          end
          rank = rank_rows left_row, right_row
          case rank
          when -1
            yield :left, left_row
            left_row = nil
          when 1
            yield :right, right_row
            right_row = nil
          when 0
            if not left_row == right_row
              yield :conflict, [left_row, right_row]
            end
            left_row = right_row = nil
          end
          # check for corresponding right rows
        end
        # if there are any unprocessed current right or left rows, yield them
        yield :left, left_row if left_row != nil
        yield :right, right_row if right_row != nil
        while right_cursor.next?
          # all remaining rows in right table exist only there --> yield them
          yield :right, right_cursor.next_row
        end
      ensure
        [left_cursor, right_cursor].each do |cursor|
          cursor.clear if cursor
        end
      end
    end
    
    # Compare the primary keys of left_row and right_row to determine their rank
    def rank_rows(left_row, right_row)
      rank = 0
      primary_key_names.any? do |key|
        rank = left_row[key] <=> right_row[key]
        rank != 0
      end
      rank
    end

    # Generates the SQL query to iterate through the given target table.
    # Note: The column & order part of the query are always generated based on left_table.
    def construct_query(target_table)
      column_names = session.left.columns(left_table).map {|column| column.name}
      "select #{column_names.join(', ')} from #{target_table} order by #{primary_key_names.join(', ')}"
    end
  end
end
