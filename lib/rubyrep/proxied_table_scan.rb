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
      @block_size ||= session.configuration.options_for_table(left_table)[:proxy_block_size]
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
    # +left_cursor+ and +right_cursor+ represent the according ProxyBlockCursor objects.
    # Yields all identified differences with
    # * diff_type
    # * row
    # #run described the yield parameters in detail.
    def compare_blocks(left_block_cursor, right_block_cursor)
      left_cursor = right_cursor = nil
      
      left_row_checksums = left_block_cursor.row_checksums
      right_row_checksums = right_block_cursor.row_checksums

      # phase 1: identify the different rows and store their primary keys
      left_diff_rows = []
      left_diff_checksums = []
      right_diff_rows = []
      right_diff_checksums = []
      i = k = 0
      while i < left_row_checksums.size or k < right_row_checksums.size
        left_keys = i < left_row_checksums.size ? left_row_checksums[i][:row_keys] : nil
        right_keys = k < right_row_checksums.size ? right_row_checksums[k][:row_keys] : nil
        rank = rank_rows left_keys, right_keys
        case rank
        when -1
          left_diff_rows << left_keys
          left_diff_checksums << left_row_checksums[i][:checksum]
          i += 1
        when 1
          right_diff_rows << right_keys
          right_diff_checksums << right_row_checksums[k][:checksum]
          k += 1
        when 0
          if left_row_checksums[i][:checksum] != right_row_checksums[k][:checksum]
            left_diff_rows << left_keys
            left_diff_checksums << left_row_checksums[i][:checksum]
            right_diff_rows << right_keys
            right_diff_checksums << right_row_checksums[k][:checksum]
          end
          i += 1
          k += 1
        end
      end
      
      # retrieve possibly existing cached rows from the block cursors
      left_row_cache = left_block_cursor.retrieve_row_cache left_diff_checksums
      right_row_cache = right_block_cursor.retrieve_row_cache right_diff_checksums
      
      # builds arrays of row keys for rows that were not included in the hash
      left_uncached_rows = []
      left_diff_rows.each_with_index do |row, i|
        left_uncached_rows << row unless left_row_cache[left_diff_checksums[i]]
      end
      right_uncached_rows = []
      right_diff_rows.each_with_index do |row, i|
        right_uncached_rows << row unless right_row_cache[right_diff_checksums[i]]
      end

      # phase 2: read all different rows and yield them
      unless left_uncached_rows.empty?
        left_cursor = session.left.create_cursor \
          ProxyRowCursor, left_table, :row_keys => left_uncached_rows
      end
      unless right_uncached_rows.empty?
        right_cursor = session.right.create_cursor \
          ProxyRowCursor, right_table, :row_keys => right_uncached_rows  
      end
      i = k = 0
      while i < left_diff_rows.size or k < right_diff_rows.size
        rank = rank_rows left_diff_rows[i], right_diff_rows[k]
        case rank
        when -1
          if left_row_cache.include? left_diff_checksums[i]
            row = Marshal.load(left_row_cache[left_diff_checksums[i]])
          else
            row = left_cursor.next_row
          end
          yield :left, row
          i += 1
        when 1
          if right_row_cache.include? right_diff_checksums[k]
            row = Marshal.load(right_row_cache[right_diff_checksums[k]])
          else
            row = right_cursor.next_row
          end
          yield :right, row
          k += 1
        when 0
          if left_row_cache.include? left_diff_checksums[i]
            left_row = Marshal.load(left_row_cache[left_diff_checksums[i]])
          else
            left_row = left_cursor.next_row
          end
          if right_row_cache.include? right_diff_checksums[k]
            right_row = Marshal.load(right_row_cache[right_diff_checksums[k]])
          else
            row = right_cursor.next_row
          end
          yield :conflict, [left_row, right_row]
          i += 1
          k += 1
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
      left_cursor = session.left.create_cursor ProxyBlockCursor, left_table, 
        :row_buffer_size => scan_options[:row_buffer_size]
      right_cursor = session.right.create_cursor ProxyBlockCursor, right_table, 
        :row_buffer_size => scan_options[:row_buffer_size]
      update_progress 0 # ensures progress bar is printed even if there are no records
      while left_cursor.next?
        left_to, left_checksum, left_progress =
          left_cursor.checksum :proxy_block_size => block_size
        _ , right_checksum, right_progress =
          right_cursor.checksum :max_row => left_to
        combined_progress = left_progress + right_progress
        if left_checksum != right_checksum
          compare_blocks left_cursor, right_cursor do |type, row|
            steps = type == :conflict ? 2 : 1
            update_progress steps
            combined_progress -= steps
            yield type, row
          end
        end
        update_progress combined_progress
      end
      while right_cursor.next?
        update_progress 1
        yield :right, right_cursor.next_row
      end
    ensure
      session.left.destroy_cursor left_cursor if left_cursor
      session.right.destroy_cursor right_cursor if right_cursor      
    end
  end
end