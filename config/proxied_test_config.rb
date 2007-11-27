load File.dirname(__FILE__) + '/test_config.rb'

RR::Initializer::run do |config|
  config.left.merge!({
    :proxy_host  => 'localhost',   
    :proxy_port => '9876',   
  })
end
