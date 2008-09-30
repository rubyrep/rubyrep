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
    end
  end
end

