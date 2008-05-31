module RR
  # Committers are classes that implement transaction policies.
  # This module provides functionality to register committers and access the
  # list of registered committers.
  # Every Committer needs to register itself with Committers#register.
  # See RR::Committers::DefaultCommitter for the methods that a Committer needs
  # to implement.
  module Committers
    # Returns a Hash of currently registered committers.
    # (Empty Hash if no connection committers were defined.)
    def self.committers
      @committers ||= {}
      @committers
    end
  
    # Registers one or multiple committers.
    # committer_hash is a Hash with 
    #   key::   The adapter symbol as used to reference the committer
    #   value:: The class implementing the committer
    def self.register(committer_hash)
      @committers ||= {}
      @committers.merge! committer_hash
    end
    
    # This committer does not do anything. This means that the default DBMS 
    # behaviour is used (for most DBMS: every DML statement (insert, update, 
    # delete) runs in it's own transaction.
    class DefaultCommitter
      
      # Register the committer
      Committers.register :default => self

      # The current Session object
      attr_accessor :session 
      
      # Name of the left table
      attr_accessor :left_table
    
      # Name of the right table
      attr_accessor :right_table
    
      # The table specific sync options
      attr_accessor :sync_options

      # A new committer is created for each table sync.
      #   * session: a Session object representing the current database session
      #   * left_table: name of the table in the left database
      #   * right_table: name of the table in the right database.
      #   * sync_options: the table specific sync options
      def initialize(session, left_table, right_table, sync_options)
        self.session, self.left_table, self.right_table, self.sync_options =
          session, left_table, right_table, sync_options
      end
      
      # Is called whenever data are modified.
      # Needs to yield so that the data changes can be executed.
      # +databases+ is an array containing +:left+ and / or +:right+ to signal
      # in which databases changes are going to be executed.
      def notify_change(databases)
        databases.each do |database|
          unless [:left, :right].include? database
            raise ArgumentError, "only :left or :right allowed"
          end
        end
        yield
      end
      
      # Is called after the table sync is completed.
      def table_sync_completed
      end
    end
    
    # Starts a transaction but does never commit it.
    # Useful during testing.
    class NeverCommitter < DefaultCommitter
      Committers.register :never_commit => self
      @@current_session = nil
      
      # Returns the last active data session
      def self.current_session
        @@current_session
      end
      
      # Saves the provided database session as class variable.
      # Purpose: the last database session stays available after the 
      # NeverCommitter is destroyed so that also later the transaction rollback
      # can still be executed.
      def self.current_session=(session)
        @@current_session = session
      end
      
      # Rolls back transactions of current session (if there is one).
      # This would be called e. g. in rspec's after(:each) to ensure that 
      # the next test case finds the original test data.
      def self.rollback_current_session
        if self.current_session
          self.current_session.left.rollback_db_transaction
          self.current_session.right.rollback_db_transaction
          self.current_session = nil
        end
      end

      # Refer to DefaultCommitter#initialize for details.
      # Starts new transactions on left and right database connectin of session.
      # Additionally rolls back transactions started in previous 
      # +NeverCommitter+ instances.
      def initialize(session, left_table, right_table, sync_options)
        super
        self.class.rollback_current_session
        self.class.current_session = session
        session.left.begin_db_transaction
        session.right.begin_db_transaction
      end
    end
    
  end
end