$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'tsort'
require 'rubyrep'

module RR
  # This class sorts a given list of tables so that tables referencing other
  # tables via foreign keys are placed behind those referenced tables.
  #
  # Rationale:
  # If tables are sorted in that sequence, the risk of foreign key violations is
  # smaller.
  class TableSorter
    include TSort

    # The active +Session+
    attr_accessor :session

    # The list of table names to be ordered
    attr_accessor :tables

    # The table dependencies.
    # Format as described e. g. here: PostgreSQLExtender#referenced_tables
    def referenced_tables
      unless @referenced_tables
        @referenced_tables = session.left.referenced_tables(tables)

        # Strip away all unrelated tables
        @referenced_tables.each_pair do |table, references|
          references.delete_if do |reference|
            not tables.include? reference
          end
        end
      end
      @referenced_tables
    end

    # Yields each table.
    # For details see standard library: TSort#sort_each_node.
    def tsort_each_node
      referenced_tables.each_key do |table|
        yield table
      end
    end

    # Yields all tables that are references by +table+.
    def tsort_each_child(table)
      referenced_tables[table].each do |reference|
        yield reference
      end
    end

    def sort
      # Note:
      # We should not use TSort#tsort as this one throws an exception if
      # there are cyclic redundancies.
      # (Our goal is to just get the best ordering that is possible and then
      # take our chances.)
      strongly_connected_components.flatten
    end

    # Initializes the TableSorter
    # * session: The active +Session+ instance
    # * tables: an array of table names
    def initialize(session, tables)
      self.session = session
      self.tables = tables
    end
  end
end