module RR
  
  class Configuration
    attr_accessor :left

    attr_accessor :right

  end

  class Initializer

    def self.reset
      @@configuration = Configuration.new
    end
    reset
    
    def self.configuration
      @@configuration
    end
    
    def self.run
      yield configuration
    end
  end

end