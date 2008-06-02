RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'postgresql',   
    :database => 'rr_left',   
    :username => 'postgres',   
    :password => 'password',   
    :host     => 'localhost'
  }

  config.right = {
    :adapter  => 'postgresql',   
    :database => 'rr_right',   
    :username => 'postgres',   
    :password => 'password',   
    :host     => 'localhost'
  }

end

unless RUBY_PLATFORM =~ /java/
  require 'rubygems'
  require 'activerecord'
  require "active_record/connection_adapters/postgresql_adapter"

  module ActiveRecord
    module ConnectionAdapters
      class PostgreSQLAdapter
        protected
        # Returns the version of the connected PostgreSQL version.
        def postgresql_version
          @postgresql_version ||=
            # Mimic PGconn.server_version behavior
          begin
            query('SELECT version()')[0][0] =~ /PostgreSQL (\d+)\.(\d+)\.(\d+)/
            ($1.to_i * 10000) + ($2.to_i * 100) + $3.to_i
          rescue
            0
          end
        end
      end
    end
  end
end
