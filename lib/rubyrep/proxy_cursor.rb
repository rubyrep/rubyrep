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
      self.cursor = connection.select_cursor(
        options.merge(:table => table, :type_cast => true)
      )
    end
    
    # Releases all ressources
    def destroy
      self.cursor.clear if self.cursor
      self.cursor = nil
    end
  end
end
