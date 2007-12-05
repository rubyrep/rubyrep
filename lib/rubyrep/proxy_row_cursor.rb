$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'digest/sha1'

require 'rubyrep'

module RR
  
  # This class is used to scan a given table range 
  # Can return rows either themselves or only their checksum
  class ProxyRowCursor < ProxyCursor
    
    # The column_name => value hash of the current row.
    attr_accessor :current_row
    
    # Creates a new cursor
    #   * session: the current proxy session
    #   * table: table_name
    def initialize(session, table)
      super
    end
    
    # Returns true if there are unprocessed rows in the table range
    def next?
      cursor.next?
    end
    
    # Returns the next row in cursor
    def next_row
      cursor.next_row
    end
    
    # Returns for the next row
    #   * a hash of :column_name => value pairs of the primary keys
    #   * checksum string for that row
    def next_row_keys_and_checksum
      self.current_row = cursor.next_row
      keys = self.current_row.reject {|key, | not primary_key_names.include? key}
      checksum = Digest::SHA1.hexdigest(Marshal.dump(self.current_row))
      return keys, checksum
    end
  end
end
