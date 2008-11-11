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

    # Loads a difference
    def load
      changes = {}
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

      if changes[OTHER_SIDE[oldest]].loaded?
        self.type = :conflict
        self.changes.replace changes
      else
        self.type = oldest
        self.changes[oldest] = changes[oldest]
      end
      
      self.loaded = true
    end

  end
end