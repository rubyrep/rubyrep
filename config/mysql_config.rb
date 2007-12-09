RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'mysql',   
    :database => 'rr_left',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :socket   => '/var/run/mysqld/mysqld.sock'
  }

  config.right = {
    :adapter  => 'mysql',   
    :database => 'rr_right',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :socket   => '/var/run/mysqld/mysqld.sock'
  }

end
