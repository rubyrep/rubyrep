module RR

  # Describes a (record specific) difference between both databases as identifed
  # via change log.
  class ReplicationDifference

    # The current Session.
    def session
      @session ||= loaders.session
    end

    # The current LoggedChangeLoaders instance
    attr_accessor :loaders
    
    # The type of the difference. Either
    # * :+left+: change in left database
    # * :+right+: change in right database
    # * :+conflict+: change in both databases
    # * :+no_diff+: changes in both databases constitute no difference
    attr_accessor :type

    # Is set to +true+ if first replication attempt failed but it should be tried again later
    attr_accessor :second_chance
    alias_method :second_chance?, :second_chance

    # A hash with keys :+left+ and / or :+right+.
    # Hash values are LoggedChange instances.
    def changes
      @changes ||= {}
    end

    # Creates a new ReplicationDifference instance.
    # +loaders+ is teh current LoggedChangeLoaders instance
    def initialize(loaders)
      self.loaders = loaders
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
      loaders.update
      changes[:left].load
      changes[:right].load
      self.type = DIFF_TYPES[changes[:left].type][changes[:right].type]
    end
    
    # Loads a difference
    def load
      change_times = {}
      [:left, :right].each do |database|
        changes[database] = LoggedChange.new loaders[database]
        change_times[database] = loaders[database].oldest_change_time
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

    # Prevents session and change loaders from going into YAML output
    def to_yaml_properties
      instance_variables.sort.reject {|var_name| ['@session', '@loaders'].include? var_name}
    end

  end
end