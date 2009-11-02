require 'rubygems'
require 'test/unit/notification'
require 'test/unit'
require 'mocha'
require 'active_support'
require 'active_support/test_case'

unless defined?(RAILS_ROOT)
 RAILS_ROOT = File.expand_path(File.join(File.dirname(__FILE__), "mocks"))
end
unless defined?(RAILS_ENV)
  RAILS_ENV = "test"
end
unless defined?(RAILS_DEFAULT_LOGGER)
  RAILS_DEFAULT_LOGGER = Logger.new(nil)
end

require File.join(File.dirname(__FILE__), "..", "init")

#turn off excessive logging
AmqpListener.set_logger do |level, to_log|
  #do nothing with it
end