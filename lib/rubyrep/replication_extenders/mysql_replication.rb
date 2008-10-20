module RR
  module ReplicationExtenders

    # Provides Mysql specific functionality for database replication
    module MysqlReplication
      RR::ReplicationExtenders.register :mysql => self

      # Creates or replaces the replication trigger function.
      # See #create_replication_trigger for a descriptions of the +params+ hash.
      def create_or_replace_replication_trigger_function(params)
        execute(<<-end_sql)
          DROP PROCEDURE IF EXISTS #{params[:trigger_name]};
        end_sql
        
        activity_check = ""
        if params[:exclude_rr_activity] then
          activity_check = <<-end_sql
            DECLARE active INT;
            SELECT count(*) INTO active FROM #{params[:activity_table]};
            IF active <> 0 THEN
              LEAVE p;
            END IF;
          end_sql
        end

        execute(<<-end_sql)
          CREATE PROCEDURE #{params[:trigger_name]}(change_key varchar(2000), change_org_key varchar(2000), change_type varchar(1))
          p: BEGIN
            #{activity_check}
            INSERT INTO #{params[:log_table]}(change_table, change_key, change_org_key, change_type, change_time)
              VALUES('#{params[:table]}', change_key, change_org_key, change_type, now());
          END;
        end_sql
        
      end

      # Returns the key clause that is used in the trigger function.
      # * +trigger_var+: should be either 'NEW' or 'OLD'
      # * +params+: the parameter hash as described in #create_rep_trigger
      def key_clause(trigger_var, params)
        "concat_ws('#{params[:key_sep]}', " +
          params[:keys].map { |key| "'#{key}', #{trigger_var}.#{key}"}.join(", ") +
          ")"
      end
      private :key_clause

      # Creates a trigger to log all changes for the given table.
      # +params+ is a hash with all necessary information:
      # * :+trigger_name+: name of the trigger
      # * :+table+: name of the table that should be monitored
      # * :+keys+: array of names of the key columns of the monitored table
      # * :+log_table+: name of the table receiving all change notifications
      # * :+activity_table+: name of the table receiving the rubyrep activity information
      # * :+key_sep+: column seperator to be used in the key column of the log table
      # * :+exclude_rr_activity+:
      #   if true, the trigger will check and filter out changes initiated by RubyRep
      def create_replication_trigger(params)
        create_or_replace_replication_trigger_function params

        %w(insert update delete).each do |action|
          execute(<<-end_sql)
            DROP TRIGGER IF EXISTS #{params[:trigger_name]}_#{action};
          end_sql

          # The created triggers can handle the case where the trigger procedure
          # is updated (that is: temporarily deleted and recreated) while the
          # trigger is running.
          # For that an MySQL internal exception is raised if the trigger
          # procedure cannot be found. The exception is caught by an trigger
          # internal handler. 
          # The handler causes the trigger to retry calling the
          # trigger procedure several times with short breaks in between.

          trigger_var = action == 'delete' ? 'OLD' : 'NEW'
          if action == 'update'
            call_statement = "CALL #{params[:trigger_name]}(#{key_clause('NEW', params)}, #{key_clause('OLD', params)}, '#{action[0,1].upcase}');"
          else
            call_statement = "CALL #{params[:trigger_name]}(#{key_clause(trigger_var, params)}, null, '#{action[0,1].upcase}');"
          end
          execute(<<-end_sql)
            CREATE TRIGGER #{params[:trigger_name]}_#{action}
              AFTER #{action} ON #{params[:table]} FOR EACH ROW BEGIN
                DECLARE number_attempts INT DEFAULT 0;
                DECLARE failed INT;
                DECLARE CONTINUE HANDLER FOR 1305 BEGIN
                  DO SLEEP(0.05);
                  SET failed = 1;
                  SET number_attempts = number_attempts + 1;
                END;
                REPEAT
                  SET failed = 0;
                  #{call_statement}
                UNTIL failed = 0 OR number_attempts >= 40 END REPEAT;
              END;
          end_sql
        end

      end

      # Removes a trigger and related trigger procedure.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table for which the trigger exists
      def drop_replication_trigger(trigger_name, table_name)
        %w(insert update delete).each do |action|
          execute "DROP TRIGGER #{trigger_name}_#{action};"
        end
        execute "DROP PROCEDURE #{trigger_name};"
      end

      # Returns +true+ if the named trigger exists for the named table.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table
      def replication_trigger_exists?(trigger_name, table_name)
        !select_all("select 1 from information_schema.triggers where trigger_schema = database() and trigger_name = '#{trigger_name}_insert' and event_object_table = '#{table_name}'").empty?
      end

      # Ensures that the sequences of the named table (normally the primary key
      # column) are generated with the correct increment and offset.
      # * +rep_prefix+:
      #   The prefix put in front of all replication related database objects as
      #   specified via Configuration#options.
      #   Is used to create the sequences table.
      # * +table_name+: name of the table
      # * +increment+: increment of the sequence
      # * +offset+: offset
      # E. g. an increment of 2 and offset of 1 will lead to generation of odd
      # numbers.
      def ensure_sequence_setup(rep_prefix, table_name, increment, offset)
        # check if the table has an auto_increment column, return if not
        sequence_row = select_one(<<-end_sql)
          show columns from #{table_name} where extra = 'auto_increment'
        end_sql
        return unless sequence_row
        column_name = sequence_row['Field']

        # check if the sequences table exists, create if necessary
        sequence_table_name = "#{rep_prefix}_sequences"
        unless tables.include?(sequence_table_name)
          create_table "#{sequence_table_name}".to_sym,
            :id => false, :options => 'ENGINE=MyISAM' do |t|
            t.column :name, :string
            t.column :current_value, :integer
            t.column :increment, :integer
            t.column :offset, :integer
          end
          ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
            ALTER TABLE "#{sequence_table_name}"
            ADD CONSTRAINT #{sequence_table_name}_pkey
            PRIMARY KEY (name)
          end_sql
        end

        # check the sequence setting, update if necessary,
        # create sequence trigger if necessary
        buffer =  10 # number of records to advance the sequence to avoid conflicts with concurrent updates
        sequence_row = select_one("select current_value, increment, offset from #{sequence_table_name} where name = '#{table_name}'")
        if sequence_row == nil
          # no sequence exists yet for the table, create it and the according
          # sequence trigger
          current_max = select_one(<<-end_sql)['current_max'].to_i
            select max(#{column_name}) as current_max from #{table_name}
          end_sql
          new_start = current_max - (current_max % increment) + buffer * increment + offset
          execute(<<-end_sql)
            insert into #{sequence_table_name}(name, current_value, increment, offset)
            values('#{table_name}', #{new_start}, #{increment}, #{offset})
          end_sql
          trigger_name = "#{rep_prefix}_#{table_name}_sequence"
          execute(<<-end_sql)
            DROP TRIGGER IF EXISTS #{trigger_name};
          end_sql

          execute(<<-end_sql)
            CREATE TRIGGER #{trigger_name}
              BEFORE INSERT ON #{table_name} FOR EACH ROW BEGIN
                IF NEW.#{column_name} = 0 THEN
                  UPDATE #{sequence_table_name}
                    SET current_value = LAST_INSERT_ID(current_value + increment);
                  SET NEW.#{column_name} = LAST_INSERT_ID();
                END IF;
              END;
          end_sql
        elsif sequence_row['increment'].to_i != increment or sequence_row['offset'].to_i != offset
          # sequence exists but with incorrect values; update it
          current_max = sequence_row['current_value'].to_i
          new_start = current_max - (current_max % increment) + buffer * increment + offset
          execute(<<-end_sql)
            update #{sequence_table_name}
            set current_value = #{new_start},
            increment = #{increment}, offset = #{offset}
            where name = '#{table_name}'
          end_sql
        end
      end

      # Removes the custom sequence setup for the specified table.
      # If no more rubyrep sequences are left, removes the sequence table.
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table
      def clear_sequence_setup(rep_prefix, table_name)
        sequence_table_name = "#{rep_prefix}_sequences"
        if tables.include?(sequence_table_name)
          trigger_name = "#{rep_prefix}_#{table_name}_sequence"
          trigger_row = select_one(<<-end_sql)
            select * from information_schema.triggers
            where trigger_schema = database()
            and trigger_name = '#{trigger_name}'
          end_sql
          if trigger_row
            execute "DROP TRIGGER #{trigger_name}"
            execute "delete from #{sequence_table_name} where name = '#{table_name}'"
            unless select_one("select * from #{sequence_table_name}")
              # no more sequences left --> delete sequence table
              drop_table sequence_table_name.to_sym
            end
          end
        end
      end
    end
  end
end

