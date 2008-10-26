$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR

  # Ensures all preconditions are met to start with replication
  class ReplicationInitializer

    # The active Session
    attr_accessor :session

    # Creates a new RepInititializer for the given Session
    def initialize(session)
      self.session = session
    end

    # Returns the options for the given table.
    # If table is +nil+, returns general options.
    def options(table = nil)
      if table
        session.configuration.options_for_table table
      else
        session.configuration.options
      end
    end

    # Creates a trigger logging all table changes
    # * database: either :+left+ or :+right+
    # * table: name of the table
    def create_trigger(database, table)
      options = self.options(table)

      params = {
        :trigger_name => "#{options[:rep_prefix]}_#{table}",
        :table => table,
        :keys => session.send(database).primary_key_names(table),
        :log_table => "#{options[:rep_prefix]}_change_log",
        :activity_table => "#{options[:rep_prefix]}_active",
        :key_sep => options[:key_sep],
        :exclude_rubyrep_activity => true,
      }

      session.send(database).create_replication_trigger params
    end

    # Returns +true+ if the replication trigger for the given table exists.
    # * database: either :+left+ or :+right+
    # * table: name of the table
    def trigger_exists?(database, table)
      trigger_name = "#{options(table)[:rep_prefix]}_#{table}"
      session.send(database).replication_trigger_exists? trigger_name, table
    end

    # Drops the replication trigger of the named table.
    # * database: either :+left+ or :+right+
    # * table: name of the table
    def drop_trigger(database, table)
      trigger_name = "#{options(table)[:rep_prefix]}_#{table}"
      session.send(database).drop_replication_trigger trigger_name, table
    end

    # Ensures that the sequences of the named table (normally the primary key
    # column) are generated with the correct increment and offset.
    # The sequence is always updated in both databases.
    # * +table_name+: name of the table
    # * +increment+: increment of the sequence
    # * +offset+: offset
    # E. g. an increment of 2 and offset of 1 will lead to generation of odd
    # numbers.
    def ensure_sequence_setup(table, increment, offset)
      table_options = options(table)
      rep_prefix = table_options[:rep_prefix]
      left_sequence_values = session.left.outdated_sequence_values \
        rep_prefix, table, increment, offset
      right_sequence_values = session.right.outdated_sequence_values \
        rep_prefix, table, increment, offset
      [:left, :right].each do |database|
        session.send(database).update_sequences \
          rep_prefix, table, increment, offset,
          left_sequence_values, right_sequence_values, table_options[:sequence_adjustment_buffer]
      end
    end

    # Restores the original sequence settings for the named table.
    # (Actually it sets the sequence increment to 1. If before, it had a
    # different value, then the restoration will not be correct.)
    # * database: either :+left+ or :+right+
    # * +table_name+: name of the table
    def clear_sequence_setup(database, table)
      session.send(database).clear_sequence_setup(
        options(table)[:rep_prefix], table
      )
    end

    # Returns +true+ if the replication log exists in the specified database.
    # * database: either :+left+ or :+right+
    def replication_log_exists?(database)
      session.send(database).tables.include? "#{options[:rep_prefix]}_change_log"
    end

    # Drops the replication log table in the specified database
    # * database: either :+left+ or :+right+
    def drop_replication_log(database)
      session.send(database).drop_table "#{options[:rep_prefix]}_change_log"
    end

    # Creates the replication log table in the specified database
    # * database: either :+left+ or :+right+
    def create_replication_log(database)
      if session.configuration.send(database)[:adapter] =~ /postgres/
        # suppress the postgres stderr output about creation of indexes
        old_message_level = session.send(database).
          select_one("show client_min_messages")['client_min_messages']
        session.send(database).execute "set client_min_messages = warning"
      end
      session.send(database).create_table "#{options[:rep_prefix]}_change_log" do |t|
        t.column :change_table, :string
        t.column :change_key, :string
        t.column :change_org_key, :string
        t.column :change_type, :string
        t.column :change_time, :timestamp
      end
      if session.configuration.send(database)[:adapter] =~ /postgres/
        session.send(database).execute "set client_min_messages = #{old_message_level}"
      end
    end
  end

end
