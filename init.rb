$:.unshift "#{File.dirname(__FILE__)}/lib"
require 'amqp_listener'

AMQP::Client.class_eval do
  
  def determine_reconnect_server_with_logging
    try_host, try_port = determine_reconnect_server_without_logging
    RAILS_DEFAULT_LOGGER.info("Connecting to #{try_host} #{try_port} (attempt #{@retry_count})")
    return [try_host, try_port]
  end
  
  alias_method_chain :determine_reconnect_server, :logging
  
end