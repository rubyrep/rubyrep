module RR
  module ConnectionExtenders

    # Provides various PostgreSQL specific functionality required by Rubyrep.
    module PostgreSQLExtender
      RR::ConnectionExtenders.register :postgresql => self
    end
  end
end

