module RR
  
  # Describes a single logged record change.
  # 
  # Note:
  # The change loading functionality depends on the current database session
  # being executed in an open database transaction.
  # Also at the end of change processing the transaction must be committed.
  class LoggedChange

    # The current LoggedChangeLoader
    attr_accessor :loader

    # The current Session
    def session
      @session ||= loader.session
    end

    # The current database (either +:left+ or +:right+)
    def database
      @database ||= loader.database
    end

    # The name of the changed table
    attr_accessor :table

    # When the first change to the record happened
    attr_accessor :first_changed_at

    # When the last change to the record happened
    attr_accessor :last_changed_at

    # Type of the change. Either :+insert+, :+update+ or :+delete+.
    attr_accessor :type

    # A column_name => value hash identifying the changed record
    attr_accessor :key

    # Only used for updates: a column_name => value hash of the original primary
    # key of the updated record
    attr_accessor :new_key

    # Creates a new LoggedChange instance.
    # * +loader+: the current LoggedChangeLoader
    # * +database+: either :+left+ or :+right+
    def initialize(loader)
      self.loader = loader
      self.type = :no_change
    end

    # A hash describing how the change state morph based on newly found change
    # records.
    # * key: String consisting of 2 letters
    #   * first letter: describes current type change (nothing, insert, update, delete)
    #   * second letter: the new change type as read of the change log table
    # * value:
    #   The resulting change type.
    # [1]: such cases shouldn't happen. but just in case, choose the most
    # sensible solution.
    TYPE_CHANGES = {
      'NI' => 'I',
      'NU' => 'U',
      'ND' => 'D',
      'II' => 'I', # [1]
      'IU' => 'I',
      'ID' => 'N',
      'UI' => 'U', # [1]
      'UU' => 'U',
      'UD' => 'D',
      'DI' => 'U',
      'DU' => 'U', # [1]
      'DD' => 'D', # [1]
    }

    # A hash translating the short 1-letter types to the according symbols
    SHORT_TYPES = {
      'I' => :insert,
      'U' => :update,
      'D' => :delete,
      'N' => :no_change
    }
    # A hash translating the symbold types to according 1 letter types
    LONG_TYPES = SHORT_TYPES.invert

    # Returns the configured key separator
    def key_sep
      @key_sep ||= session.configuration.options[:key_sep]
    end

    # Returns a column_name => value hash based on the provided +raw_key+ string
    # (which is a string in the format as read directly from the change log table).
    def key_to_hash(raw_key)
      result = {}
      #raw_key.split(key_sep).each_slice(2) {|a| result[a[0]] = a[1]}
      raw_key.split(key_sep).each_slice(2) {|field_name, value| result[field_name] = value}
      result
    end

    # Loads the change as per #table and #key. Works if the LoggedChange instance
    # is totally new or was already loaded before.
    def load
      current_type = LONG_TYPES[type]

      org_key = new_key || key
      # change to key string as can be found in change log table
      org_key = session.send(database).primary_key_names(table).map do |key_name|
        "#{key_name}#{key_sep}#{org_key[key_name]}"
      end.join(key_sep)
      current_key = org_key

      while change = loader.load(table, current_key)

        new_type = change['change_type']
        current_type = TYPE_CHANGES["#{current_type}#{new_type}"]

        self.first_changed_at ||= change['change_time']
        self.last_changed_at = change['change_time']

        if change['change_type'] == 'U' and change['change_new_key'] != current_key
          current_key = change['change_new_key']
        end
      end

      self.type = SHORT_TYPES[current_type]
      self.new_key = nil
      if type == :update
        self.key ||= key_to_hash(org_key)
        self.new_key = key_to_hash(current_key)
      else
        self.key = key_to_hash(current_key)
      end
    end

    # Loads the change with the specified key for the named +table+.
    # * +table+: name of the table
    # * +key+: a column_name => value hash for all primary key columns of the table
    def load_specified(table, key)
      self.table = table
      self.key = key
      load
    end

    # Loads the oldest available change
    def load_oldest
      begin
        change = loader.oldest_change
        break unless change
        self.key = key_to_hash(change['change_key'])
        self.table = change['change_table']
        load
      end until type != :no_change
    end

    # Prevents session from going into YAML output
    def to_yaml_properties
      instance_variables.sort.reject {|var_name| ['@session', '@loader'].include? var_name}
    end

  end
end