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
    
    # Initiate a query for the specified row range.
    # +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def prepare_fetch(options = {})
      self.cursor = TypeCastingCursor.new(
        session.connection,
        table, 
        session.connection.select_cursor(construct_query(options))
      )
    end
    
    # Quotes the given value. The value is assumed to belong to the given column name.
    def quote_column_value(column, value)
      session.quote_value(table, column, value)
    end
    
    # Creates an SQL query string based on the given +options+.
    # +options+ is a hash that can contain any of the following:
    #   * +:from+: nil OR the hash of primary key => value pairs designating the start of the selection
    #   * +:to+: nil OR the hash of primary key => value pairs designating the end of the selection
    #   * +:row_keys+: an array of primary key => value hashes specify the target rows.
    def construct_query(options = {})
      options.each_key do |key| 
        raise "options must only include :from, :to or :row_keys" unless [:from, :to, :row_keys].include? key
      end
      query = "select #{session.column_names(table).join(', ')} from #{table}"
      query << " where" unless options.empty?
      first_condition = true
      if options[:from]
        first_condition = false
        query << row_condition(options[:from], '>=')
      end
      if options[:to]
        query << ' and' unless first_condition
        first_condition = false
        query << row_condition(options[:to], '<=')
      end
      if options[:row_keys]
        query << ' and' unless first_condition
        if options[:row_keys].empty?
          query << ' false'
        else
          query << ' (' << primary_key_names.join(', ') << ') in ('
          first_key = true
          options[:row_keys].each do |row|
            query << ', ' unless first_key
            first_key = false
            query << '(' << primary_key_names.map {|key| quote_column_value(key, row[key])}.join(', ') << ')'
          end
          query << ')'
        end
      end
      query << " order by #{primary_key_names.join(', ')}"

      query
    end
    
    # Generates an sql condition string based on
    #   * +row+: a hash of primary key => value pairs designating the target row
    #   * +condition+: the type of sql condition (something like '>=' or '=', etc.)
    def row_condition(row, condition)
      query_part = ""
      query_part << ' (' << primary_key_names.join(', ') << ') ' << condition
      query_part << ' (' << primary_key_names.map {|key| quote_column_value(key, row[key])}.join(', ') << ')'
      query_part
    end
    private :row_condition
    
    # Releases all ressources
    def destroy
      self.cursor.clear if self.cursor
      self.cursor = nil
    end
  end
end
