module RR
  module ReplicationExtenders

    # Provides PostgreSQL specific functionality for database replication
    module PostgreSQLReplication
      RR::ReplicationExtenders.register :postgresql => self

      # Returns the key clause that is used in the trigger function.
      # * +trigger_var+: should be either 'NEW' or 'OLD'
      # * +params+: the parameter hash as described in #create_rep_trigger
      def key_clause(trigger_var, params)
        params[:keys].
          map { |key| "'#{key}#{params[:key_sep]}' || #{trigger_var}.#{key}"}.
          join(" || '#{params[:key_sep]}' || ")
      end
      private :key_clause

      # Creates or replaces the replication trigger function.
      # See #create_replication_trigger for a descriptions of the +params+ hash.
      def create_or_replace_replication_trigger_function(params)
        # first check, if PL/SQL is already activated and if not, do so.
        if select_all("select lanname from pg_language where lanname = 'plpgsql'").empty?
          execute "CREATE LANGUAGE plpgsql"
        end

        activity_check = ""
        if params[:exclude_rr_activity] then
          activity_check = <<-end_sql
            PERFORM ACTIVE FROM #{params[:activity_table]};
            IF FOUND THEN
              RETURN NULL;
            END IF;
          end_sql
        end

        # now create the trigger
        execute(<<-end_sql)
          CREATE OR REPLACE FUNCTION #{params[:trigger_name]}() RETURNS TRIGGER AS $change_trigger$
            BEGIN
              #{activity_check}
              IF (TG_OP = 'DELETE') THEN
                INSERT INTO #{params[:log_table]}(change_table, change_key, change_type, change_time) 
                  SELECT '#{params[:table]}', #{key_clause('OLD', params)}, 'D', now();
              ELSIF (TG_OP = 'UPDATE') THEN
                INSERT INTO #{params[:log_table]}(change_table, change_key, change_org_key, change_type, change_time)
                  SELECT '#{params[:table]}', #{key_clause('NEW', params)}, #{key_clause('OLD', params)}, 'U', now();
              ELSIF (TG_OP = 'INSERT') THEN
                INSERT INTO #{params[:log_table]}(change_table, change_key, change_type, change_time)
                  SELECT '#{params[:table]}', #{key_clause('NEW', params)}, 'I', now();
              END IF;
              RETURN NULL; -- result is ignored since this is an AFTER trigger
            END;
          $change_trigger$ LANGUAGE plpgsql;
        end_sql

      end

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

        execute(<<-end_sql)
          CREATE TRIGGER #{params[:trigger_name]}
          AFTER INSERT OR UPDATE OR DELETE ON #{params[:table]}
              FOR EACH ROW EXECUTE PROCEDURE #{params[:trigger_name]}();
        end_sql
      end

      # Removes a trigger and related trigger procedure.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table for which the trigger exists
      def drop_replication_trigger(trigger_name, table_name)
        execute "DROP TRIGGER #{trigger_name} ON #{table_name};"
        execute "DROP FUNCTION #{trigger_name}();"
      end

      # Returns +true+ if the named trigger exists for the named table.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table
      def replication_trigger_exists?(trigger_name, table_name)
        search_path = select_one("show search_path")['search_path']
        schemas = search_path.split(/,/).map { |p| quote(p) }.join(',')
        !select_all(<<-end_sql).empty?
          select 1 from information_schema.triggers
          where event_object_schema in (#{schemas})
          and trigger_name = '#{trigger_name}'
          and event_object_table = '#{table_name}'
        end_sql
      end

      # Ensures that the sequences of the named table (normally the primary key
      # column) are generated with the correct increment and offset.
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table
      # * +increment+: increment of the sequence
      # * +offset+: offset
      # E. g. an increment of 2 and offset of 1 will lead to generation of odd
      # numbers.
      def ensure_sequence_setup(rep_prefix, table_name, increment, offset)
        sequence_names = select_all(<<-end_sql).map { |row| row['relname'] }
          select s.relname
          from pg_class as t
          join pg_depend as r on t.oid = r.refobjid
          join pg_class as s on r.objid = s.oid
          and s.relkind = 'S'
          and t.relname = '#{table_name}'
        end_sql
        sequence_names.each do |sequence_name|
          val1 = select_one("select nextval('#{sequence_name}')")['nextval'].to_i
          val2 = select_one("select nextval('#{sequence_name}')")['nextval'].to_i
          unless val2 == val1 and val2 % increment == offset
            buffer =  10 # number of records to advance the sequence to avoid conflicts with concurrent updates
            new_start = val2 - (val2 % increment) + buffer * increment + offset
            execute(<<-end_sql)
              alter sequence "#{sequence_name}" increment by #{increment} restart with #{new_start}
            end_sql
          end
        end
      end

      # Restores the original sequence settings.
      # (Actually it sets the sequence increment to 1. If before, it had a
      # different value, then the restoration will not be correct.)
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table
      def clear_sequence_setup(rep_prefix, table_name)
        sequence_names = select_all(<<-end_sql).map { |row| row['relname'] }
          select s.relname
          from pg_class as t
          join pg_depend as r on t.oid = r.refobjid
          join pg_class as s on r.objid = s.oid
          and s.relkind = 'S'
          and t.relname = '#{table_name}'
        end_sql
        sequence_names.each do |sequence_name|
          execute(<<-end_sql)
              alter sequence "#{sequence_name}" increment by 1
          end_sql
        end
      end
    end
  end
end

