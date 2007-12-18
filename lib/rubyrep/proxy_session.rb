$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'

module RR

  # This class represents a remote rubyrep session.
  # DatabaseProxy creates one for each client connection.
  # Creates and holds expensive objects like e. g. database connections.
  class ProxySession
    
    # The database connection
    attr_accessor :connection
    
    # Hash to register cursors.
    # Purpose:
    #   Objects only referenced remotely via DRb can be garbage collected.
    #   We register them in this hash to protect them from unintended garbage collection.
    attr_accessor :cursors
    
    # 2-level Hash of table_name => column_name => Column objects
    attr_accessor :table_columns
    
    # Simply calls the select_one function of the proxied database connection
    def select_one(sql, name = nil)
      self.connection.select_one(sql, name)
    end
    
    # Returns a Hash of currently registerred cursors
    def cursors
      @cursors ||= {}
    end
    
    # Store a cursor in the register to protect it from the garbage collector.
    def save_cursor(cursor)
      cursors[cursor] = cursor
    end
    
    # Create a session on the proxy side according to provided configuration hash and proxy_options hash
    def initialize(config)
      self.connection = ConnectionExtenders.db_connect config
    end
    
    # Destroys the session
    def destroy
      self.connection.disconnect!
      
      cursors.each_key do |cursor|
        cursor.destroy
      end
      cursors.clear
    end
    
    # Quotes the given value. It is assumed that the value belongs to the specified column name and table name.
    # Caches the column objects for higher speed.
    def quote_value(table, column, value)
      self.table_columns ||= {}
      unless table_columns.include? table
        table_columns[table] = {}
        connection.columns(table).each {|c| table_columns[table][c.name] = c}
      end
      connection.quote value, table_columns[table][column]
    end
    
    # Create a cursor for the given table.
    #   * +cursor_class+: should specify the Cursor class (e. g. ProxyBlockCursor or ProxyRowCursor).
    #   * +table+: name of the table 
    #   * +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def create_cursor(cursor_class, table, options = {})
      cursor = cursor_class.new self, table
      cursor.prepare_fetch options
      save_cursor cursor
      cursor
    end
    
    # Destroys the provided cursor and removes it from the register
    def destroy_cursor(cursor)
      cursor.destroy
      cursors.delete cursor
    end
    
    # returns the columns of the given table name
    def column_names(table)
      connection.columns(table).map {|column| column.name}
    end
    
    # Returns an array of primary key names for the given table name
    def primary_key_names(table)
      connection.primary_key_names table
    end
  end
end
