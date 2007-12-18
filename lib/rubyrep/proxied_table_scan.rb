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
    
    # Compares the left and right rows between (not including) from and (including) to
    # 'from' and 'to' each are hashes of :column_name => column_value pairs containing the primary key columns
    def compare_blocks(from, to)
      left_cursor = right_cursor = nil
      left_cursor = session.left.create_cursor ProxyRowCursor, left_table, :from => from, :to => to
      right_cursor = session.right.create_cursor ProxyRowCursor, right_table, :from => from, :to => to
      left_keys = right_keys = nil
      while left_keys or right_keys or left_cursor.next? or right_cursor.next?
        # if there is no current left row, load the next one
        if !left_keys and left_cursor.next?
          left_keys, left_checksum = left_cursor.next_row_keys_and_checksum 
        end
        # if there is no current right row, _try_ to load the next one
        if !right_keys and right_cursor.next?
          right_keys, right_checksum = right_cursor.next_row_keys_and_checksum
        end
          
        # continue with next record if left_cursor is at 'from'
        if left_keys == from
          left_keys = nil
          next
        end
        # continue with next record if right_cursor is at 'from'
        if right_keys == from
          right_keys = nil
          next
        end

        rank = rank_rows left_keys, right_keys
        case rank
        when -1
          yield :left, left_cursor.current_row
          left_keys = nil
        when 1
          yield :right, right_cursor.current_row
          right_keys = nil
        when 0
          if not left_checksum == right_checksum
            yield :conflict, [left_cursor.current_row, right_cursor.current_row]
          end
          left_keys = right_keys = nil
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
          compare_blocks last_left_to, left_to do |type, row|
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