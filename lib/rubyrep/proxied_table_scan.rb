module RR
  
  # Scans two tables for differences. Goes through a RubyRep Proxy to minimize network load.
  # Doesn't have any reporting functionality by itself. 
  # Instead ProxiedTableScan#run yields all the differences for the caller to do with as it pleases.
  # Usage:
  #   1. Create a new ProxiedTableScan object and hand it all necessary information
  #   2. Call ProxiedTableScan#run to do the actual comparison
  #   3. The block handed to ProxiedTableScan#run receives all differences
  class ProxiedTableScan < TableScan
    
    # returns block size to use for table scanning
    def block_size
      @block_size ||= session.configuration.proxy_options[:block_size]
    end
  
    # Creates a new ProxiedTableScan instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      raise "#{self.class.name} only works with proxied sessions" unless session.proxied?

      super
    end
    
    # Compares the specified left and right rows.
    # +left_row_checksums+ and +right_row_checksums+ each are arrays of row checksums
    # as returned by ProxyBlockCursor#row_checksums
    def compare_blocks(left_row_checksums, right_row_checksums)
      left_cursor = right_cursor = nil

      # phase 1: identify the different rows and put store their primary keys
      left_diff_rows = []
      right_diff_rows = []
      left_index = right_index = 0
      while left_index < left_row_checksums.size or right_index < right_row_checksums.size
        left_keys = left_index < left_row_checksums.size ? left_row_checksums[left_index][:row_keys] : nil
        right_keys = right_index < right_row_checksums.size ? right_row_checksums[right_index][:row_keys] : nil
        rank = rank_rows left_keys, right_keys
        case rank
        when -1
          left_diff_rows << left_keys
          left_index += 1
        when 1
          right_diff_rows << right_keys
          right_index += 1
        when 0
          if left_row_checksums[left_index][:checksum] != right_row_checksums[right_index][:checksum]
            left_diff_rows << left_keys
            right_diff_rows << right_keys
          end
          left_index += 1
          right_index += 1
        end
      end

      # phase 2: read all different rows and yield them
      left_cursor = session.left.create_cursor ProxyRowCursor, left_table, :row_keys => left_diff_rows
      right_cursor = session.right.create_cursor ProxyRowCursor, right_table, :row_keys => right_diff_rows
      left_row = right_row = nil
      while left_row or left_cursor.next? or right_row or right_cursor.next?
        # if there is no current left row, load the next one
        if !left_row and left_cursor.next?
          left_row = left_cursor.next_row 
        end
        # if there is no current right row, _try_ to load the next one
        if !right_row and right_cursor.next?
          right_row = right_cursor.next_row
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
          yield :conflict, [left_row, right_row]
          left_row = right_row = nil
        end
      end
    ensure
      session.left.destroy_cursor left_cursor if left_cursor
      session.right.destroy_cursor right_cursor if right_cursor
    end

    # Runs the table scan.
    # Calls the block for every found difference.
    # Differences are yielded with 2 parameters
    #   * type: describes the difference, either :left (row only in left table), :right (row only in right table) or :conflict
    #   * row: for :left or :right cases a hash describing the row; for :conflict an array of left and right row
    def run(&blck)
      left_cursor = right_cursor = nil
      left_cursor = session.left.create_cursor ProxyBlockCursor, self.left_table
      right_cursor = session.right.create_cursor ProxyBlockCursor, self.right_table
      last_left_to = nil
      while left_cursor.next?
        left_to, left_checksum = left_cursor.checksum :block_size => block_size

        # note: I don't actually need right_to; only used to stuff the according return value somewhere
        right_to, right_checksum = right_cursor.checksum :max_row => left_to 

        if left_checksum != right_checksum
          compare_blocks left_cursor.row_checksums, right_cursor.row_checksums do |type, row|
            yield type, row
          end
        end
        last_left_to = left_to
      end
      while right_cursor.next?
        yield :right, right_cursor.next_row
      end
    ensure
      session.left.destroy_cursor left_cursor if left_cursor
      session.right.destroy_cursor right_cursor if right_cursor      
    end
  end
end