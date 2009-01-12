# Used as component of a rubyrep config file.
# Defines connection parameters to the mysql databases.

RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'mysql',   
    :database => 'rr_left',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :port     => 3306,
    :encoding => 'utf8'
  }

  config.right = {
    :adapter  => 'mysql',   
    :database => 'rr_right',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :port     => 3306,
    :encoding => 'utf8'
  }

end
