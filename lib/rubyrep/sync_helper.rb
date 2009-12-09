module RR

  # Provides helper functionality for the table syncers.
  # The methods exposed by this class are intended to provide a stable interface
  # for third party syncers.
  class SyncHelper

    include LogHelper

    # The current +TableSync+ instance
    attr_accessor :table_sync

    # The active +Session+
    def session; table_sync.session; end

    # Name of the left table
    def left_table; table_sync.left_table; end

    # Name of the right table
    def right_table; table_sync.right_table; end

    # A hash with
    # :+left+: name of the table in the left database
    # :+right+: name of the table in the right database
    def tables
      @tables ||= {:left => left_table, :right => right_table}
    end

    # Given a column_name => value hash of a full row, returns a
    # column_name => value hash of the primary key columns.
    # * +row+: the full row
    # Returns
    def extract_key(row)
      row.reject {|column, value| not primary_key_names.include? column }
    end

    # Sync options for the current table sync
    def sync_options; @sync_options ||= table_sync.sync_options; end

    # Delegates to Committers::BufferedCommitter#insert_record
    def insert_record(database, table, values)
      committer.insert_record(database, tables[database], values)
    end

    # Delegates to Committers::BufferedCommitter#update_record
    def update_record(database, table, values, old_key = nil)
      committer.update_record(database, tables[database], values, old_key)
    end

    # Delegates to Committers::BufferedCommitter#delete_record
    def delete_record(database, table, values)
      committer.delete_record(database, tables[database], values)
    end

    # Return the committer, creating it if not yet there.
    def committer
      unless @committer
        committer_class = Committers::committers[sync_options[:committer]]
        @committer = committer_class.new(session)
      end
      @committer
    end
    private :committer

    # Checks if the event log table already exists and creates it if necessary
    def ensure_event_log
      unless @ensured_event_log
        ReplicationInitializer.new(session).ensure_event_log
        @ensured_event_log = true
      end
    end

    # Returns an array of primary key names for the synced table
    def primary_key_names
      @primary_key_names ||= session.left.primary_key_names(left_table)
    end
    private :primary_key_names

    # Logs the outcome of a replication into the replication log table.
    # * +row+: a column_name => value hash for at least the primary keys of the record
    # * +type+: string describing the type of the sync
    # * +outcome+: string describing what's done about the sync
    # * +details+: string with further details regarding the sync
    def log_sync_outcome(row, type, outcome, details = nil)
      ensure_event_log
      if primary_key_names.size == 1
        key = row[primary_key_names[0]]
      else
        key_parts = primary_key_names.map do |column_name|
          %Q("#{column_name}"=>#{row[column_name].to_s.inspect})
        end
        key = key_parts.join(', ')
      end
      sync_outcome, sync_details = fit_description_columns(outcome, details)

      session.left.insert_record "#{sync_options[:rep_prefix]}_logged_events", {
        :activity => 'sync',
        :change_table => left_table,
        :diff_type => type.to_s,
        :change_key => key,
        :left_change_type => nil,
        :right_change_type => nil,
        :description => sync_outcome,
        :long_description => sync_details,
        :event_time => Time.now,
        :diff_dump => nil
      }
    end

    # Asks the committer (if it exists) to finalize any open transactions
    # +success+ should be true if there were no problems, false otherwise.
    def finalize(success = true)
      @committer.finalize(success) if @committer
    end
    
    # Creates a new SyncHelper for the given +TableSync+ instance.
    def initialize(table_sync)
      self.table_sync = table_sync
    end
  end
end