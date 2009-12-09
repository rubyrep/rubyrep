module RR

  # Provides helper functionality for replicators.
  # The methods exposed by this class are intended to provide a stable interface
  # for third party replicators.
  class ReplicationHelper

    include LogHelper

    # The current +ReplicationRun+ instance
    attr_accessor :replication_run

    # The active +Session+
    def session; replication_run.session; end

    # Current options
    def options; @options ||= session.configuration.options; end

    # Returns the options for the specified table name.
    # * +table+: name of the table (left database version)
    def options_for_table(table)
      @options_for_table ||= {}
      unless @options_for_table.include? table
        @options_for_table[table] = session.configuration.options_for_table(table)
      end
      @options_for_table[table]
    end

    # Delegates to Session#corresponding_table
    def corresponding_table(db_arm, table); session.corresponding_table(db_arm, table); end

    # Returns +true+ if a new transaction was started since the last
    # insert / update / delete.
    def new_transaction?
      committer.new_transaction?
    end

    # Delegates to Committers::BufferedCommitter#insert_record
    def insert_record(database, table, values)
      committer.insert_record(database, table, values)
    end

    # Delegates to Committers::BufferedCommitter#update_record
    def update_record(database, table, values, old_key = nil)
      committer.update_record(database, table, values, old_key)
    end

    # Delegates to Committers::BufferedCommitter#delete_record
    def delete_record(database, table, values)
      committer.delete_record(database, table, values)
    end

    # Loads the specified record. Returns an according column_name => value hash.
    # Parameters:
    # * +database+: either :+left+ or :+right+
    # * +table+: name of the table
    # * +key+: A column_name => value hash for all primary key columns.
    def load_record(database, table, key)
      cursor = session.send(database).select_cursor(
        :table => table,
        :row_keys => [key],
        :type_cast => true
      )
      row = nil
      row = cursor.next_row if cursor.next?
      cursor.clear
      row
    end

    # The current Committer
    attr_reader :committer
    private :committer

    # Asks the committer (if it exists) to finalize any open transactions
    # +success+ should be true if there were no problems, false otherwise.
    def finalize(success = true)
      committer.finalize(success)
    end

    # Converts the row values into their proper types as per table definition.
    # * +table+: name of the table after whose columns is type-casted.
    # * +row+: A column_name => value hash of the row
    # Returns a copy of the column_name => value hash (with type-casted values).
    def type_cast(table, row)
      @table_columns ||= {}
      unless @table_columns.include?(table)
        column_array = session.left.columns(table)
        column_hash = {}
        column_array.each {|column| column_hash[column.name] = column}
        @table_columns[table] = column_hash
      end
      columns = @table_columns[table]
      type_casted_row = {}
      row.each_pair do |column_name, value|
        type_casted_row[column_name] = columns[column_name].type_cast(value)
      end
      type_casted_row
    end

    # Logs the outcome of a replication into the replication log table.
    # * +diff+: the replicated ReplicationDifference
    # * +outcome+: string summarizing the outcome of the replication
    # * +details+: string with further details regarding the replication
    def log_replication_outcome(diff, outcome, details = nil)
      table = diff.changes[:left].table
      key = diff.changes[:left].key
      if key.size == 1
        key = key.values[0]
      else
        key_parts = session.left.primary_key_names(table).map do |column_name|
          %Q("#{column_name}"=>#{key[column_name].to_s.inspect})
        end
        key = key_parts.join(', ')
      end
      rep_outcome, rep_details = fit_description_columns(outcome, details)
      diff_dump = diff.to_yaml[0...ReplicationInitializer::DIFF_DUMP_SIZE]
      
      session.left.insert_record "#{options[:rep_prefix]}_logged_events", {
        :activity => 'replication',
        :change_table => table,
        :diff_type => diff.type.to_s,
        :change_key => key,
        :left_change_type => (diff.changes[:left] ? diff.changes[:left].type.to_s : nil),
        :right_change_type => (diff.changes[:right] ? diff.changes[:right].type.to_s : nil),
        :description => rep_outcome,
        :long_description => rep_details,
        :event_time => Time.now,
        :diff_dump => diff_dump
      }
    end
    
    # Creates a new SyncHelper for the given +TableSync+ instance.
    def initialize(replication_run)
      self.replication_run = replication_run

      # Creates the committer. Important as it gives the committer the
      # opportunity to start transactions
      committer_class = Committers::committers[options[:committer]]
      @committer = committer_class.new(session)
    end
  end
end