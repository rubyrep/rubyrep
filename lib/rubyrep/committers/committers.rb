module RR
  # Committers are classes that implement transaction policies.
  # This module provides functionality to register committers and access the
  # list of registered committers.
  # Every Committer needs to register itself with Committers#register.
  # Each Committer must implement at the following methods:
  #
  #   # Creates a new committer
  #   #   * session: a Session object representing the current database session
  #   def initialize(session)
  #
  #   # Inserts the specified record in the specified +database+ (either :left or :right).
  #   # +table+ is the name of the target table.
  #   # +values+ is a hash of column_name => value pairs.
  #   def insert_record(database, values)
  #
  #   # Updates the specified record in the specified +database+ (either :left or :right).
  #   # +table+ is the name of the target table.
  #   # +values+ is a hash of column_name => value pairs.
  #   # +old_key+ is a column_name => value hash with the original primary key.
  #   # If +old_key+ is +nil+, then the primary key must be contained in +values+.
  #   def update_record(database, values, old_key)
  #
  #   # Deletes the specified record in the specified +database+ (either :left or :right).
  #   # +table+ is the name of the target table.
  #   # +values+ is a hash of column_name => value pairs. (Only the primary key
  #   # values will be used and must be included in the hash.)
  #   def delete_record(database, values)
  #
  #   # Is called after the last insert / update / delete query.
  #   def finalize
  #
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
      
      # A hash holding the proxy connections
      # E. g. {:left => <left connection>, :right => <right connection>}
      attr_accessor :connections
    
      # A new committer is created for each table sync.
      #   * session: a Session object representing the current database session
      def initialize(session)
        self.session = session
        self.connections = {:left => session.left, :right => session.right}
      end

      # Returns +true+ if a new transaction was started since the last
      # insert / update / delete.
      def new_transaction?
        false
      end
      
      # Inserts the specified record in the specified +database+ (either :left or :right).
      # +table+ is the name of the target table.
      # +values+ is a hash of column_name => value pairs.
      def insert_record(database, table, values)
        connections[database].insert_record(table, values)
      end
      
      # Updates the specified record in the specified +database+ (either :left or :right).
      # +table+ is the name of the target table.
      # +values+ is a hash of column_name => value pairs.
      # # +old_key+ is a column_name => value hash with the original primary key.
      # If +old_key+ is +nil+, then the primary key must be contained in +values+.
      def update_record(database, table, values, old_key = nil)
        connections[database].update_record(table, values, old_key)
      end
      
      # Deletes the specified record in the specified +database+ (either :left or :right).
      # +table+ is the name of the target table.
      # +values+ is a hash of column_name => value pairs. (Only the primary key
      # values will be used and must be included in the hash.)
      def delete_record(database, table, values)
        connections[database].delete_record(table, values)
      end
      
      # Is called after the last insert / update / delete query.
      # +success+ should be true if there were no problems, false otherwise.
      def finalize(success = true)
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
      def initialize(session)
        super
        self.class.rollback_current_session
        self.class.current_session = session
        session.left.begin_db_transaction
        session.right.begin_db_transaction
      end
    end
    
  end
end