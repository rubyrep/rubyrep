# Used as component of a rubyrep config file.
# Defines connection parameters to the postgresql databases.

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