module RR
  module Committers

    # This committer periodically commits transactions. It can be used for
    # pre-replication syncs as it
    # * updates the activity marker table.
    # * switches existing triggers to filter out rubyrep activity
    class BufferedCommitter < DefaultCommitter

      # Register the committer
      Committers.register :buffered_commit => self

      # Unless overwritten via configuration, transactions are commited after the
      # given number of record changes
      DEFAULT_COMMIT_FREQUENCY = 1000

      # Switches the trigger mode of the specified +table+ in the specified
      # +database+ to ignore rubyrep activity.
      # * +database+: identifying the database (either :+left+ or :+right+)
      # * +table+: name of the table
      def exclude_rr_activity(database, table)
        trigger_mode_switcher.exclude_rr_activity database, table
      end

      # Returns the TriggerModeSwitcher (creates it if necessary)
      def trigger_mode_switcher
        @trigger_mode_switcher ||= TriggerModeSwitcher.new session
      end

      # Returns the name of the activity marker table
      def activity_marker_table
        @activity_marker_table ||= "#{session.configuration.options[:rep_prefix]}_running_flags"
      end

      # Returns +true+ if the activity marker table should be maintained.
      def maintain_activity_status?
        unless @activity_status_checked
          @activity_status_checked = true
          @maintain_activity_status = session.left.tables.include?(activity_marker_table)
        end
        @maintain_activity_status
      end

      # Returns the number of changes, after which the open transactions should
      # be committed and new transactions be started.
      def commit_frequency
        unless @commit_frequency
          @commit_frequency = session.configuration.options[:commit_frequency]
          @commit_frequency ||= DEFAULT_COMMIT_FREQUENCY
        end
        @commit_frequency
      end

      # Commits the open transactions in both databases. Before committing,
      # clears the rubyrep activity marker.
      def commit_db_transactions
        [:left, :right].each do |database|
          if maintain_activity_status?
            session.send(database).execute("delete from #{activity_marker_table}")
          end
          session.send(database).commit_db_transaction
        end
      end

      # Begins new transactions in both databases. After starting the transaction,
      # marks the activity of rubyrep.
      def begin_db_transactions
        [:left, :right].each do |database|
          session.send(database).begin_db_transaction
          if maintain_activity_status?
            session.send(database).execute("insert into #{activity_marker_table} values(1)")
          end
        end
      end

      # Rolls back the open transactions in both databases.
      def rollback_db_transactions
        session.left.rollback_db_transaction
        session.right.rollback_db_transaction
      end

      # Commits the open tranactions and starts new one if the #commit_frequency
      # number of record changes have been executed.
      def commit
        @change_counter ||= 0
        @change_counter += 1
        if @change_counter == commit_frequency
          @change_counter = 0
          commit_db_transactions
          begin_db_transactions
        end
      end

      # Returns +true+ if a new transaction was started since the last
      # insert / update / delete.
      def new_transaction?
        @change_counter == 0
      end

      # A new committer is created for each table sync.
      # * session: a Session object representing the current database session
      def initialize(session)
        super
        begin_db_transactions
      end

      # Inserts the specified record in the specified database.
      # * +database+: identifying the database (either :+left+ or :+right+)
      # * +table+: name of the table
      # * +values+: a hash of column_name => value pairs.
      def insert_record(database, table, values)
        exclude_rr_activity database, table
        super
        commit
      end

      # Updates the specified record in the specified database.
      # * +database+: identifying the database (either :+left+ or :+right+)
      # * +table+: name of the table
      # * +values+: a hash of column_name => value pairs.
      # * +old_key+:
      #   A column_name => value hash identifying original primary key.
      #   If +nil+, then the primary key must be contained in +values+.
      def update_record(database, table, values, old_key = nil)
        exclude_rr_activity database, table
        super
        commit
      end

      # Deletes the specified record in the specified database.
      # * +database+: identifying the database (either :+left+ or :+right+)
      # * +table+: name of the table
      # * +values+: a hash of column_name => value pairs (must only contain primary key columns).
      def delete_record(database, table, values)
        exclude_rr_activity database, table
        super
        commit
      end

      # Is called after the last insert / update / delete query.
      # * +success+: should be true if there were no problems, false otherwise.
      def finalize(success = true)
        if success
          commit_db_transactions
        else
          rollback_db_transactions
        end
      end
    end
  end
end