module RR

  # Scans two tables for differences.
  # Doesn't have any reporting functionality by itself. 
  # Instead DirectTableScan#run yields all the differences for the caller to do with as it pleases.
  # Usage:
  #   1. Create a new DirectTableScan object and hand it all necessary information
  #   2. Call DirectTableScan#run to do the actual comparison
  #   3. The block handed to DirectTableScan#run receives all differences
  class DirectTableScan < TableScan
    include TableScanHelper

    # The TypeCastingCursor for the left table
    attr_accessor :left_caster
    
    # The TypeCastingCursor for the right table
    attr_accessor :right_caster

    # Creates a new DirectTableScan instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      super
    end
    
    # Runs the table scan.
    # Calls the block for every found difference.
    # Differences are yielded with 2 parameters
    #   * type: describes the difference, either :left (row only in left table), :right (row only in right table) or :conflict
    #   * row: For :left or :right cases a hash describing the row; for :conflict an array of left and right row.
    #          A row is a hash of column_name => value pairs.
    def run(&blck)
      left_cursor = right_cursor = nil
      left_cursor = session.left.select_cursor(
        :table => left_table,
        :row_buffer_size => scan_options[:row_buffer_size],
        :type_cast => true
      )
      right_cursor = session.right.select_cursor(
        :table => right_table,
        :row_buffer_size => scan_options[:row_buffer_size],
        :type_cast => true
      )
      left_row = right_row = nil
      update_progress 0 # ensures progress bar is printed even if there are no records
      while left_row or right_row or left_cursor.next? or right_cursor.next?
        # if there is no current left row, _try_ to load the next one
        left_row ||= left_cursor.next_row if left_cursor.next?
        # if there is no current right row, _try_ to load the next one
        right_row ||= right_cursor.next_row if right_cursor.next?
        rank = rank_rows left_row, right_row
        case rank
        when -1
          yield :left, left_row
          left_row = nil
          update_progress 1
        when 1
          yield :right, right_row
          right_row = nil
          update_progress 1
        when 0
          update_progress 2
          if not left_row == right_row
            yield :conflict, [left_row, right_row]
          end
          left_row = right_row = nil
        end
        # check for corresponding right rows
      end
    ensure
      [left_cursor, right_cursor].each {|cursor| cursor.clear if cursor}
    end
  end
end
