$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR
  # Provides shared functionality for ProxyRowCursor and ProxyBlockCursor
  class ProxyCursor
    
    # The current ProxySession.
    attr_accessor :session
    
    # The name of the current table.
    attr_accessor :table
    
    # Array of primary key names for current table.
    attr_accessor :primary_key_names
    
    # The current cursor.
    attr_accessor :cursor
    
    # Shared initializations 
    #   * session: the current proxy session
    #   * table: table_name
    def initialize(session, table)
      self.session = session
      self.table = table
      self.primary_key_names = session.primary_key_names table
    end
    
    # Initiate a query for the specified row range
    #   * from: nil or the hash of primary key => value pairs designating the start of the selection
    #   * to: nil OR the hash of primary key => value pairs designating the start of the selection
    def prepare_fetch(from = nil, to = nil)
      self.cursor = TypeCastingCursor.new(
        session.connection, 
        table, 
        session.connection.select_cursor(construct_query(from, to))
      )
    end
    
    # Quotes the given value. The value is assumed to belong to the given column name.
    def quote_column_value(column, value)
      session.quote_value(table, column, value)
    end
    
    # Creates an SQL query string based on the given
    #   * from: nil or the hash of primary key => value pairs designating the start of the selection
    #   * to: nil OR the hash of primary key => value pairs designating the end of the selection
    def construct_query(from = nil, to = nil)
      query = "select #{session.column_names(table).join(', ')} from #{table}"
      query << " where" if from or to
      if from
        query << ' (' << primary_key_names.join(', ') << ') >='
        query << ' (' << primary_key_names.map {|key| quote_column_value(key, from[key])}.join(', ') << ')'
      end
      if to
        query << ' and' if from
        query << ' (' << primary_key_names.join(', ') << ') <='
        query << ' (' << primary_key_names.map {|key| quote_column_value(key, to[key])}.join(', ') << ')'
      end
      query << " order by #{primary_key_names.join(', ')}"

      query
    end
    
    # Releases all ressources
    def destroy
      self.cursor.clear if self.cursor
      self.cursor = nil
    end
  end
end
