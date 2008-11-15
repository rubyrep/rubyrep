module RR

  # Provides helper functionality for replicators.
  # The methods exposed by this class are intended to provide a stable interface
  # for third party replicators.
  class ReplicationHelper

    # The current +ReplicationRun+ instance
    attr_accessor :replication_run

    # The active +Session+
    def session; replication_run.session; end

    # Current options
    def options; @options ||= session.configuration.options; end

    # Delegates to Session#corresponding_table
    def corresponding_table(db_arm, table); session.corresponding_table(db_arm, table); end

    # Delegates to Committer#insert_record
    def insert_record(database, table, values)
      committer.insert_record(database, table, values)
    end

    # Delegates to Committer#insert_record
    def update_record(database, table, values, old_key = nil)
      committer.update_record(database, table, values, old_key)
    end

    # Delegates to Committer#insert_record
    def delete_record(database, table, values)
      committer.delete_record(database, table, values)
    end

    # Loads the specified record. Returns an according column_name => value hash.
    # Parameters:
    # * +database+: either :+left+ or :+right+
    # * +table+: name of the table
    # * +key+: A column_name => value hash for all primary key columns.
    def load_record(database, table, key)
      query = session.send(database).table_select_query(table, :row_keys => [key])
      cursor = TypeCastingCursor.new(
        session.send(database), table,
        session.send(database).select_cursor(query)
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