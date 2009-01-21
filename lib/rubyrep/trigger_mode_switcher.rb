require 'set'

module RR

  # Switches rubyrep triggers between "exclude rubyrep activity" modes.
  class TriggerModeSwitcher

    # Keeps track of all the triggers.
    # This is a hash with 2 keys: :+left+ and :+right+.
    # Each of these entries is a Set containing table names.
    def triggers
      @triggers ||= {
        :left => Set.new,
        :right => Set.new
      }
    end
    
    # The active Session
    attr_accessor :session

    def initialize(session)
      self.session = session
    end

    # Does the actual switching of the trigger mode.
    # * +database+: either :+left+ or :+right+
    # * +table+: name of the table
    # * +exclude_rr_activity+: the new trigger mode (either +true+ or +false+)
    def switch_trigger_mode(database, table, exclude_rr_activity)
      options = session.configuration.options
      if session.send(database).replication_trigger_exists? "#{options[:rep_prefix]}_#{table}", table
        params = {
          :trigger_name => "#{options[:rep_prefix]}_#{table}",
          :table => table,
          :keys => session.send(database).primary_key_names(table),
          :log_table => "#{options[:rep_prefix]}_pending_changes",
          :activity_table => "#{options[:rep_prefix]}_running_flags",
          :key_sep => options[:key_sep],
          :exclude_rr_activity => exclude_rr_activity,
        }
        session.send(database).create_or_replace_replication_trigger_function(params)
      end
    end

    # Switches the trigger of the named table to "exclude rubyrep activity" mode.
    # Only switches if it didn't do so already for the table.
    # * +database+: either :+left+ or :+right+
    # * +table+: name of the table
    def exclude_rr_activity(database, table)
      switch_trigger_mode(database, table, true) if triggers[database].add? table
    end

    # Restores all switched triggers to not exclude rubyrep activity
    def restore_triggers
      [:left, :right].each do |database|
        triggers[database].each do |table|
          switch_trigger_mode database, table, false
        end
        triggers[database].clear
      end
    end
  end
end