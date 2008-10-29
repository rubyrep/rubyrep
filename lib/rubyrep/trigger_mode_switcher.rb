module RR

  # Switches rubyrep triggers between "exclude rubyrep activity" modes.
  class TriggerModeSwitcher

    # Keeps track of all the triggers.
    # Is a hash with
    # * key: name of the left datbase table for the trigger
    # * value: a table pair hash
    # value is a hash with
    # * :+left+: name of the left table of the trigger
    # * :+right+: name of the right table of the trigger
    attr_accessor :triggers

    # The active Session
    attr_accessor :session

    def initialize(session)
      self.session = session
      self.triggers = {}
    end

    # Does the actual switching of the trigger mode
    # * +left_table+: name of the left database table for the trigger
    # * +right_table+: name of the matching right database table
    # * +exclude_rr_activity+: the new trigger mode (either +true+ or +false+)
    def switch_trigger_mode(left_table, right_table, exclude_rr_activity)
      options = session.configuration.options_for_table(left_table)
      [:left, :right].each do |database|
        table = database == :left ? left_table : right_table
        if session.send(database).replication_trigger_exists? "#{options[:rep_prefix]}_#{table}", table
          params = {
            :trigger_name => "#{options[:rep_prefix]}_#{table}",
            :table => table,
            :keys => session.left.primary_key_names(left_table),
            :log_table => "#{options[:rep_prefix]}_change_log",
            :activity_table => "#{options[:rep_prefix]}_active",
            :key_sep => options[:key_sep],
            :exclude_rr_activity => exclude_rr_activity,
          }
          session.send(database).create_or_replace_replication_trigger_function(params)
        end
      end
    end

    # Switches the trigger of the named table to "exclude rubyrep activity" mode
    def exclude_rr_activity(left_table, right_table)
      unless triggers.include?(left_table)
        switch_trigger_mode(left_table, right_table, true)
        triggers[left_table] = {
          :left => left_table,
          :right => right_table,
        }
      end
    end

    # Restores all switched triggers to not exclude rubyrep activity
    def restore_triggers
      triggers.each_value do |table_pair|
        switch_trigger_mode(table_pair[:left], table_pair[:right], false)
      end
      triggers.clear
    end
  end
end