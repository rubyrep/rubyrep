load File.dirname(__FILE__) + '/test_config.rb'

# $start_proxy_as_external_process = true

RR::Initializer::run do |config|
  config.left.merge!({
    :proxy_host  => 'localhost',   
    :proxy_port => '9876',   
  })

  config.proxy_options = {:block_size => 2}
end
