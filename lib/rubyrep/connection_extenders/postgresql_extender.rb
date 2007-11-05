module RR
  module ConnectionExtenders

    module PostgreSQLExtender
      RR::ConnectionExtenders.register :postgresql => self
    end
  end
end

