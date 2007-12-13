RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'mysql',   
    :database => 'rr_left',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :port     => 3306
  }

  config.right = {
    :adapter  => 'mysql',   
    :database => 'rr_right',   
    :username => 'root',   
    :password => '',   
    :host     => 'localhost',
    :port     => 3306
  }

end
