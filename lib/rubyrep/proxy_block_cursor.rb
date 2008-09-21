$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'digest/sha1'

require 'rubyrep'

module RR
  
  # This class is used to scan a table in blocks.
  # Calculates the checksums of the scanned blocks.
  class ProxyBlockCursor < ProxyCursor
    
    include TableScanHelper
    
    # The current Digest
    attr_accessor :digest
    
    # nil if the last run of the checksum method left no unprocessed row.
    # Otherwise the left over row of that checksum run
    attr_accessor :last_row
    
    # Returns an array of checksums for each encounters row.
    # Each array element is a Hash with the following elements:
    # * +:row_keys+: A primary key => value hash identifying the row
    # * +:checksum+: the checksum for this row
    attr_accessor :row_checksums
    
    # The maximum total size (in bytes) up to which rows will be cached
    attr_accessor :max_row_cache_size
    
    # A byte counter of many bytes of row data have already been cached
    attr_accessor :current_row_cache_size
    
    # A hash of cached rows consisting of row checksum => row dump pairs.
    attr_accessor :row_cache
        
    # Creates a new cursor
    # * session: the current proxy session
    # * table: table_name
    def initialize(session, table)
      self.max_row_cache_size = 1000000 # this size should be sufficient as long as table doesn't contain blobs
      super
    end
    
    # Returns true if the current cursor has unprocessed rows
    def next?
      last_row != nil or cursor.next?
    end
    
    # Returns the cursor's next row
    def next_row
      if self.last_row
        row, self.last_row = self.last_row, nil
      else
        row = cursor.next_row
      end
      row
    end
    
    # Returns a hash of row checksum => row dump pairs for the +checksums+ 
    # in the provided array
    def retrieve_row_cache(checksums)
      row_dumps = {}
      checksums.each do |checksum|
        row_dumps[checksum] = row_cache[checksum] if row_cache.include? checksum
      end      
      row_dumps
    end
    
    # Updates block / row checksums and row cache with the given +row+.
    def update_checksum(row)
      dump = Marshal.dump(row)
      
      # updates row checksum array
      row_keys = row.reject {|key, | not primary_key_names.include? key}
      checksum = Digest::SHA1.hexdigest(dump)
      self.row_checksums << {:row_keys => row_keys, :checksum => checksum}

      # update the row cache (unless maximum cache size limit has already been reached)
      if current_row_cache_size + dump.size < max_row_cache_size
        self.current_row_cache_size += dump.size
        row_cache[checksum] = dump
      end
      
      # update current total checksum
      self.digest << dump
    end
    
    # Reinitializes the row checksum array and the total checksum
    def reset_checksum
      self.row_checksums = []
      self.current_row_cache_size = 0
      self.row_cache = {}
      self.digest = Digest::SHA1.new
    end
    
    # Returns the current checksum
    def current_checksum
      self.digest.hexdigest
    end
    
    # Calculates the checksum from the current row up to the row specified by options.
    # options is a hash including either 
    # * :+proxy_block_size+: The number of rows to scan.
    # * :+max_row+: A row hash of primary key columns specifying the maximum record to scan.
    # Returns multiple parameters:
    # * last row read
    # * checksum
    # * number of processed records
    def checksum(options = {})
      reset_checksum
      return_row = row = nil
      row_count = 0

      if options.include? :proxy_block_size
        block_size = options[:proxy_block_size]
        raise ":proxy_block_size must be greater than 0" unless block_size > 0
        while row_count < block_size and next?
          row = next_row
          update_checksum(row)
          row_count += 1
        end
        return_row = row
      elsif options.include? :max_row
        max_row = options[:max_row]
        while next?
          row = next_row
          rank = rank_rows row, max_row
          if rank > 0 
            # row > max_row ==> save the current row and break off
            self.last_row = row
            break
          end
          row_count += 1
          update_checksum(row)
          return_row, row = row, nil
        end  
      else
        raise "options must include either :proxy_block_size or :max_row"
      end
      return_keys = return_row.reject {|key, | not primary_key_names.include? key} if return_row
      return return_keys, current_checksum, row_count
    end
  end
end
