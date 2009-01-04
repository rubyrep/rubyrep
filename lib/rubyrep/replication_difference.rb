module RR

  # Describes a (record specific) difference between both databases as identifed
  # via change log.
  class ReplicationDifference

    # The current Session.
    attr_accessor :session
    
    # The type of the difference. Either
    # * :+left+: change in left database
    # * :+right+: change in right database
    # * :+conflict+: change in both databases
    # * :+no_diff+: changes in both databases constitute no difference
    attr_accessor :type

    # A hash with keys :+left+ and / or :+right+.
    # Hash values are LoggedChange instances.
    def changes
      @changes ||= {}
    end

    # Creates a new ReplicationDifference instance.
    # +session+ is the current Session.
    def initialize(session)
      self.session = session
    end

    # Should be set to +true+ if this ReplicationDifference instance was
    # successfully loaded.
    attr_writer :loaded

    # Returns +true+ if a replication difference was loaded
    def loaded?
      @loaded
    end

    # Shortcut to calculate the "other" database.
    OTHER_SIDE = {
      :left => :right,
      :right => :left
    }

    # Resulting diff type based on types of left changes (outer hash) and right
    # changes (inner hash)
    DIFF_TYPES = {
      :insert =>    {:insert => :conflict, :update => :conflict, :delete => :conflict,  :no_change => :left},
      :update =>    {:insert => :conflict, :update => :conflict, :delete => :conflict,  :no_change => :left},
      :delete =>    {:insert => :conflict, :update => :conflict, :delete => :no_change, :no_change => :left},
      :no_change => {:insert => :right,    :update => :right,    :delete => :right,     :no_change => :no_change}
    }

    # Amends a difference according to new entries in the change log table
    def amend
      session.reload_changes
      changes[:left].load
      changes[:right].load
      self.type = DIFF_TYPES[changes[:left].type][changes[:right].type]
    end
    
    # Loads a difference
    def load
      change_times = {}
      [:left, :right].each do |database|
        changes[database] = LoggedChange.new session, database
        change_times[database] = changes[database].oldest_change_time
      end
      return if change_times[:left] == nil and change_times[:right] == nil

      oldest = nil
      [:left, :right].each do |database|
        oldest = OTHER_SIDE[database] if change_times[database] == nil
      end
      oldest ||= change_times[:left] <= change_times[:right] ? :left : :right
      changes[oldest].load_oldest

      changes[OTHER_SIDE[oldest]].load_specified(
        session.corresponding_table(oldest, changes[oldest].table),
        changes[oldest].key)

      self.type = DIFF_TYPES[changes[:left].type][changes[:right].type]
      self.loaded = true
    end

    # Prevents session from going into YAML output
    def to_yaml_properties
      instance_variables.sort.reject {|var_name| var_name == '@session'}
    end

  end
end