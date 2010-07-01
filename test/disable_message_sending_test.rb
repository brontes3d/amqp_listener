require File.join(File.dirname(__FILE__), "test_helper")

require 'activerecord'
ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :database => ":memory:")

class DisableMessageSendingTest < ActiveSupport::TestCase
  
  def setup
    super
  end
  
  def teardown
    AmqpListener.cleanup
    Thread.current[:mq] = nil
    AMQP.instance_eval{ @conn = nil }
    AMQP.instance_eval{ @closing = false }
    AMQP::Client.class_eval{ @retry_count = 0 }
    AMQP::Client.class_eval{ @server_to_select = 0 }    
  end
  
  def test_send
    expect_messaging_is_disabled
    AmqpListener.disable_message_sending_while do
      AmqpListener.send("some_q", "some_message")
    end
  end
  
  def test_send_reenabled
    bunny_stub = stub()
    Bunny.stubs(:new).returns(bunny_stub)
    bunny_stub.stubs(:start).returns(true)
    q_stub = stub()
    bunny_stub.expects(:queue).with("different_q", :durable => true, :auto_delete => false).returns(q_stub)
    q_stub.expects(:publish).with("different_message", :persistent => true)
    
    AmqpListener.disable_message_sending_while do
      AmqpListener.send("some_q", "some_message")
    end
    AmqpListener.send("different_q", "different_message")  
  end
  
  def test_run
    expect_messaging_is_disabled
    AmqpListener.load_listeners
    TestListener
    AmqpListener.disable_message_sending_while do
      AmqpListener.run
    end
  end
  
  def test_send_to_exchange
    expect_messaging_is_disabled
    AmqpListener.disable_message_sending_while do
      AmqpListener.send_to_exchange("routing.key", "message")
    end
  end
  
  def test_subscribe_with_bunny
    expect_messaging_is_disabled
    AmqpListener.disable_message_sending_while do
      AmqpListener.with_bunny do |bunny|
        queue = bunny.queue("some_q")
        queue.subscribe do |msg|
          puts "got: " + msg.inspect
        end
      end
    end
  end
  
  def test_send_with_bunny
    expect_messaging_is_disabled
    AmqpListener.disable_message_sending_while do
      AmqpListener.with_bunny do |bunny|
        AmqpListener.send("some_q", "some_message", false, {}, {}, bunny)
      end
    end
  end
  
  private
  
  def expect_messaging_is_disabled
    bunny_stub = stub
    Bunny.stubs(:new).returns(bunny_stub)
    bunny_stub.expects(:start).never
    bunny_stub.expects(:queue).never
    bunny_stub.expects(:publish).never
    MQ.expects(:queue).never
    MQ.expects(:publish).never    
  end
  
end
