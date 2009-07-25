module RR

  # Wraps an existing cursor.
  # Purpose: send regular updates to the installed TaskSweeper
  class NoisyCursor
    # The original cusor
    attr_accessor :org_cursor

    # The installed task sweeper
    attr_accessor :sweeper

    # Create a new NoisyCursor.
    # * cursor: the original cursor
    # * sweeper: the target TaskSweeper
    def initialize(cursor, sweeper)
      self.org_cursor = cursor
      self.sweeper = sweeper
    end

    # Delegate the uninteresting methods to the original cursor
    def next?; org_cursor.next? end
    def clear; org_cursor.clear end

    # Returns the row as a column => value hash and moves the cursor to the next row.
    def next_row
      sweeper.ping
      row = org_cursor.next_row
      sweeper.ping
      row
    end
  end

  # Modifies ProxyConnections to send regular pings to an installed TaskSweeper
  module NoisyConnection

    # The installed TaskSweeper
    attr_accessor :sweeper

    # Modifies ProxyConnection#select_cursor to wrap the returned cursor
    # into a NoisyCursor.
    def select_cursor(options)
      sweeper.ping
      org_cursor = super
      sweeper.ping
      NoisyCursor.new(org_cursor, sweeper)
    end

    # Wraps ProxyConnection#insert_record to update the TaskSweeper
    def insert_record(table, values)
      sweeper.ping
      result = super
      sweeper.ping
      result
    end

    # Wraps ProxyConnection#update_record to update the TaskSweeper
    def update_record(table, values, org_key = nil)
      sweeper.ping
      result = super
      sweeper.ping
      result
    end

    # Wraps ProxyConnection#delete_record to update the TaskSweeper
    def delete_record(table, values)
      sweeper.ping
      result = super
      sweeper.ping
      result
    end

    # Wraps ProxyConnection#commit_db_transaction to update the TaskSweeper
    def commit_db_transaction
      sweeper.ping
      result = super
      sweeper.ping
      result
    end
  end
end