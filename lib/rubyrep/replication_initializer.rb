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
        :log_table => "#{options[:rep_prefix]}_pending_changes",
        :activity_table => "#{options[:rep_prefix]}_running_flags",
        :key_sep => options[:key_sep],
        :exclude_rr_activity => false,
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
    # column) are generated with the correct increment and offset in both
    # left and right database.
    # The sequence is always updated in both databases.
    # * +table_pair+: a hash of names of corresponding :left and :right tables
    # * +increment+: increment of the sequence
    # * +left_offset+: offset of table in left database
    # * +right_offset+: offset of table in right database
    # E. g. an increment of 2 and offset of 1 will lead to generation of odd
    # numbers.
    def ensure_sequence_setup(table_pair, increment, left_offset, right_offset)
      table_options = options(table_pair[:left])
      if table_options[:adjust_sequences]
        rep_prefix = table_options[:rep_prefix]
        left_sequence_values = session.left.sequence_values rep_prefix, table_pair[:left]
        right_sequence_values = session.right.sequence_values rep_prefix, table_pair[:right]
        [:left, :right].each do |database|
          offset = database == :left ? left_offset : right_offset
          session.send(database).update_sequences \
            rep_prefix, table_pair[database], increment, offset,
            left_sequence_values, right_sequence_values, table_options[:sequence_adjustment_buffer]
        end
      end
    end

    # Restores the original sequence settings for the named table.
    # (Actually it sets the sequence increment to 1. If before, it had a
    # different value, then the restoration will not be correct.)
    # * database: either :+left+ or :+right+
    # * +table_name+: name of the table
    def clear_sequence_setup(database, table)
      table_options = options(table)
      if table_options[:adjust_sequences]
        session.send(database).clear_sequence_setup(
          table_options[:rep_prefix], table
        )
      end
    end

    # Returns +true+ if the change log exists in the specified database.
    # * database: either :+left+ or :+right+
    def change_log_exists?(database)
      session.send(database).tables.include? "#{options[:rep_prefix]}_pending_changes"
    end

    # Returns +true+ if the replication log exists.
    def event_log_exists?
      session.left.tables.include? "#{options[:rep_prefix]}_logged_events"
    end

    # Drops the change log table in the specified database
    # * database: either :+left+ or :+right+
    def drop_change_log(database)
      session.send(database).drop_table "#{options[:rep_prefix]}_pending_changes"
    end

    # Drops the replication log table.
    def drop_event_log
      session.left.drop_table "#{options[:rep_prefix]}_logged_events"
    end

    # Size of the replication log column diff_dump
    DIFF_DUMP_SIZE = 2000

    # Size fo the event log column 'description'
    DESCRIPTION_SIZE = 255

    # Size of the event log column 'long_description'
    LONG_DESCRIPTION_SIZE = 1000

    # Ensures that create_table and related statements don't print notices to
    # stdout. Then restored original message setting.
    # * +database+: either :+left+ or :+right+
    def silence_ddl_notices(database)
      if session.configuration.send(database)[:adapter] =~ /postgres/
        old_message_level = session.send(database).
          select_one("show client_min_messages")['client_min_messages']
        session.send(database).execute "set client_min_messages = warning"
      end
      yield
    ensure
      if session.configuration.send(database)[:adapter] =~ /postgres/
        session.send(database).execute "set client_min_messages = #{old_message_level}"
      end
    end

    # Creates the replication log table.
    def create_event_log
      silence_ddl_notices(:left) do
        table_name = "#{options[:rep_prefix]}_logged_events"
        session.left.create_table "#{options[:rep_prefix]}_logged_events"
        session.left.add_column table_name, :activity, :string
        session.left.add_column table_name, :change_table, :string
        session.left.add_column table_name, :diff_type, :string
        session.left.add_column table_name, :change_key, :string
        session.left.add_column table_name, :left_change_type, :string
        session.left.add_column table_name, :right_change_type, :string
        session.left.add_column table_name, :description, :string, :limit => DESCRIPTION_SIZE
        session.left.add_column table_name, :long_description, :string, :limit => LONG_DESCRIPTION_SIZE
        session.left.add_column table_name, :event_time, :timestamp
        session.left.add_column table_name, :diff_dump, :string, :limit => DIFF_DUMP_SIZE
        session.left.remove_column table_name, 'id'
        session.left.add_big_primary_key table_name, 'id'
      end
    end

    # Creates the change log table in the specified database
    # * database: either :+left+ or :+right+
    def create_change_log(database)
      silence_ddl_notices(database) do
        connection = session.send(database)
        table_name = "#{options[:rep_prefix]}_pending_changes"
        connection.create_table table_name
        connection.add_column table_name, :change_table, :string
        connection.add_column table_name, :change_key, :string
        connection.add_column table_name, :change_new_key, :string
        connection.add_column table_name, :change_type, :string
        connection.add_column table_name, :change_time, :timestamp
        connection.remove_column table_name, 'id'
        connection.add_big_primary_key table_name, 'id'
      end
    end

    # Adds to the current session's configuration an exclusion of rubyrep tables.
    def exclude_rubyrep_tables
      session.configuration.exclude_rubyrep_tables
    end

    # Checks in both databases, if the activity marker tables exist and if not,
    # creates them.
    def ensure_activity_markers
      table_name = "#{options[:rep_prefix]}_running_flags"
      [:left, :right].each do |database|
        connection = session.send(database)
        unless connection.tables.include? table_name
          silence_ddl_notices(database) do
            connection.create_table table_name
            connection.add_column table_name, :active, :integer
            connection.remove_column table_name, 'id'
          end
        end
      end
    end

    # Checks if the event log table already exists and creates it if necessary
    def ensure_event_log
      create_event_log unless event_log_exists?
    end

    # Checks in both databases, if the change log tables exists and creates them
    # if necessary
    def ensure_change_logs
      [:left, :right].each do |database|
        create_change_log(database) unless change_log_exists?(database)
      end
    end

    # Checks in both databases, if the infrastructure tables (change log, event
    # log) exist and creates them if necessary.
    def ensure_infrastructure
      ensure_activity_markers
      ensure_change_logs
      ensure_event_log
    end

    # Checks in both databases, if the change_log tables exist. If yes, drops them.
    def drop_change_logs
      [:left, :right].each do |database|
        drop_change_log(database) if change_log_exists?(database)
      end
    end

    # Checks in both databases, if the activity_marker tables exist. If yes, drops them.
    def drop_activity_markers
      table_name = "#{options[:rep_prefix]}_running_flags"
      [:left, :right].each do |database|
        if session.send(database).tables.include? table_name
          session.send(database).drop_table table_name
        end
      end
    end

    # Removes all rubyrep infrastructure tables from both databases.
    def drop_infrastructure
      drop_event_log if event_log_exists?
      drop_change_logs
      drop_activity_markers
    end

    # Checks for tables that have triggers but are not in the list of configured
    # tables. Removes triggers and restores sequences of those tables.
    # * +configured_table_pairs+:
    #   An array of table pairs (e. g. [{:left => 'xy', :right => 'xy2'}]).
    def restore_unconfigured_tables(configured_table_pairs = session.configured_table_pairs)
      [:left, :right].each do |database|
        configured_tables = configured_table_pairs.map {|table_pair| table_pair[database]}
        unconfigured_tables = session.send(database).tables - configured_tables
        unconfigured_tables.each do |table|
          if trigger_exists?(database, table)
            drop_trigger(database, table)
            session.send(database).execute(
              "delete from #{options[:rep_prefix]}_pending_changes where change_table = '#{table}'")
          end
          clear_sequence_setup(database, table)
        end
      end
    end

    # Calls the potentially provided :+after_init+ handler after infrastructure
    # tables are created.
    def call_after_infrastructure_setup_handler
      handler = session.configuration.options[:after_infrastructure_setup]
      handler.call(session) if handler
    end

    # Prepares the database / tables for replication.
    def prepare_replication
      exclude_rubyrep_tables

      puts "Verifying RubyRep tables"
      ensure_infrastructure

      call_after_infrastructure_setup_handler

      puts "Checking for and removing rubyrep triggers from unconfigured tables"
      restore_unconfigured_tables

      puts "Verifying rubyrep triggers of configured tables"
      unsynced_table_pairs = []
      table_pairs = session.sort_table_pairs(session.configured_table_pairs)
      table_pairs.each do |table_pair|
        table_options = options(table_pair[:left])
        ensure_sequence_setup table_pair,
          table_options[:sequence_increment],
          table_options[:left_sequence_offset],
          table_options[:right_sequence_offset]

        unsynced = false
        [:left, :right].each do |database|
          unless trigger_exists? database, table_pair[database]
            create_trigger database, table_pair[database]
            unsynced = true
          end
        end
        if unsynced and table_options[:initial_sync]
          unsynced_table_pairs << table_pair
        end
      end
      unsynced_table_specs = unsynced_table_pairs.map do |table_pair|
        "#{table_pair[:left]}, #{table_pair[:right]}"
      end

      unless unsynced_table_specs.empty?
        puts "Executing initial table syncs"
        runner = SyncRunner.new
        runner.session = session
        runner.options = {:table_specs => unsynced_table_specs}
        runner.execute
      end

      puts "Starting replication"
    end
  end

end
