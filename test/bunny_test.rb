require File.join(File.dirname(__FILE__), "test_helper")

require 'activerecord'
ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :database => ":memory:")

load File.expand_path(File.dirname(__FILE__) + "/mocks/schema.rb")
require File.expand_path(File.dirname(__FILE__) + '/mocks/lock.rb')

class BunnyTest < ActiveSupport::TestCase
  
  def setup
    AmqpListener.load_listeners
    AmqpListener.exception_handler do |arg1, arg2, arg3|
      #ignore
    end
  end

  def teardown
    AmqpListener.cleanup
    Thread.current[:mq] = nil
    AMQP.instance_eval{ @conn = nil }
    AMQP.instance_eval{ @closing = false }
    AMQP::Client.class_eval{ @retry_count = 0 }
    AMQP::Client.class_eval{ @server_to_select = 0 }
  end
  
  class BunnyStubTimeout
    def start
      raise Timeout::Error
    end
  end
  
  def test_bunny_retry_timeout
    hosts_tried = []
    Bunny.stubs(:new).returns(BunnyStubTimeout.new).with do |args|
      hosts_tried << args[:host]
      true
    end
    assert_raises(Timeout::Error) do
      AmqpListener.send("test_q", "test_message")
    end
    assert_equal(["nonexistanthost", "nonexistant1", "nonexistant2"], hosts_tried)
  end

  class BunnyStubRuntime
    def start
      raise "hi"
    end
  end
  
  def test_bunny_retry_runtime
    hosts_tried = []
    Bunny.stubs(:new).returns(BunnyStubRuntime.new).with do |args|
      hosts_tried << args[:host]
      true
    end
    assert_raises(RuntimeError) do
      AmqpListener.send("test_q", "test_message")
    end
    assert_equal(["nonexistanthost", "nonexistant1", "nonexistant2"], hosts_tried)
  end
  
  
end
