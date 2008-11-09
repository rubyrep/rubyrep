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
      rep_prefix = table_options[:rep_prefix]
      left_sequence_values = session.left.outdated_sequence_values \
        rep_prefix, table_pair[:left], increment, left_offset
      right_sequence_values = session.right.outdated_sequence_values \
        rep_prefix, table_pair[:right], increment, right_offset
      [:left, :right].each do |database|
        offset = database == :left ? left_offset : right_offset
        session.send(database).update_sequences \
          rep_prefix, table_pair[database], increment, offset,
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
      session.send(database).create_table "#{options[:rep_prefix]}_change_log", :id => false do |t|
        t.column :change_table, :string
        t.column :change_key, :string
        t.column :change_new_key, :string
        t.column :change_type, :string
        t.column :change_time, :timestamp
      end
      session.send(database).add_big_primary_key "#{options[:rep_prefix]}_change_log", 'id'
      if session.configuration.send(database)[:adapter] =~ /postgres/
        session.send(database).execute "set client_min_messages = #{old_message_level}"
      end
    end

    # Adds to the current session's configuration an exclusion of rubyrep tables.
    def exclude_rubyrep_tables
      r = Regexp.new "^#{options[:rep_prefix]}_.*"
      session.configuration.exclude_tables r
    end

    # Checks in both databases, if the activity marker tables exist and if not,
    # creates them.
    def ensure_activity_marker_tables
      table_name = "#{options[:rep_prefix]}_active"
      [:left, :right].each do |database|
        unless session.send(database).tables.include? table_name
          session.send(database).create_table table_name, :id => false do |t|
            t.column :active, :integer
          end
        end
      end
    end

    # Checks in both databases, if the replication log tables exist and if not,
    # creates them.
    def ensure_replication_log_tables
      [:left, :right].each do |database|
        create_replication_log(database) unless replication_log_exists?(database)
      end
    end

    # Prepares the database / tables for replication.
    def prepare_replication
      exclude_rubyrep_tables

      puts "Verifying RubyRep tables"
      ensure_activity_marker_tables
      ensure_replication_log_tables

      unsynced_table_pairs = []

      puts "Verifying sequence and trigger setup of replicated tables"
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
        unsynced_table_pairs << table_pair if unsynced
      end
      unsynced_table_specs = unsynced_table_pairs.map do |table_pair|
        "#{table_pair[:left]}, #{table_pair[:right]}"
      end

      puts "Executing initial table syncs" unless unsynced_table_specs.empty?
      runner = SyncRunner.new
      runner.session = session
      runner.options = {:table_specs => unsynced_table_specs}
      runner.execute
    end
  end

end
