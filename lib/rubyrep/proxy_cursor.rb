$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR
  # Provides shared functionality for ProxyRowCursor and ProxyBlockCursor
  class ProxyCursor
    
    # The current ProxyConnection.
    attr_accessor :connection
    
    # The name of the current table.
    attr_accessor :table
    
    # Array of primary key names for current table.
    attr_accessor :primary_key_names
    
    # The current cursor.
    attr_accessor :cursor
    
    # Shared initializations 
    #   * connection: the current proxy connection
    #   * table: table_name
    def initialize(connection, table)
      self.connection = connection
      self.table = table
      self.primary_key_names = connection.primary_key_names table
    end
    
    # Initiate a query for the specified row range.
    # +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def prepare_fetch(options = {})
      self.cursor = TypeCastingCursor.new(
        connection,
        table, 
        connection.select_cursor(construct_query(options))
      )
    end
    
    # Creates an SQL query string based on the given +options+.
    # +options+ is a hash that can contain any of the following:
    #   * +:from+: nil OR the hash of primary key => value pairs designating the start of the selection
    #   * +:to+: nil OR the hash of primary key => value pairs designating the end of the selection
    #   * +:row_keys+: an array of primary key => value hashes specify the target rows.
    def construct_query(options = {})
      connection.table_select_query(table, options)
    end
    
    # Releases all ressources
    def destroy
      self.cursor.clear if self.cursor
      self.cursor = nil
    end
  end
end
