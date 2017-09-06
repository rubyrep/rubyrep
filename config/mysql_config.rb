# Used as component of a rubyrep config file.
# Defines connection parameters to the mysql databases.

RR::Initializer::run do |config|

  mysql_user = RUBY_PLATFORM == 'java' && 'root' || `whoami`.strip

  config.left = {
    :adapter  => 'mysql2',
    :database => 'rr_left',   
    :username => mysql_user,
    :password => '',   
    :host     => 'localhost',
    :port     => 3306,
    :encoding => 'utf8'
  }

  config.right = {
    :adapter  => 'mysql2',
    :database => 'rr_right',   
    :username => mysql_user,
    :password => '',   
    :host     => 'localhost',
    :port     => 3306,
    :encoding => 'utf8'
  }

end
