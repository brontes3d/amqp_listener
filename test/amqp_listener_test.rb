require 'test/unit'
require File.join(File.dirname(__FILE__), "test_helper")

class AmqpListenerTest < ActiveSupport::TestCase
  
  def setup
    AmqpListener.load_listeners
    TestListener.should_raise = false
    TestListener.side_effect = false
  end
  
  def test_recieve_a_message
    AMQP.stubs(:start).yields
    header_stub = stub(:ack => true)
    q_stub = stub()
    q_stub.stubs(:subscribe).yields(header_stub, "message body")
    MQ.stubs(:queue).returns(q_stub)
    
    AmqpListener.load_listeners
    TestListener.any_instance.expects(:on_message).with("message body")
    AmqpListener.run
  end
  
  def test_send_a_message
    AMQP.stubs(:start).yields
    q_stub = stub()
    MQ.expects(:queue).with("test_q", :durable => true).returns(q_stub)
    q_stub.expects(:publish).with("test message", :persistent => true)
    AmqpListener.send("test_q", "test message")
  end
  
  def test_json_messages
    hash = {"x" => "y", "a" => "b"}
    json = hash.to_json
    
    AMQP.stubs(:start).yields
    header_stub = stub(:ack => true)
    q_stub = stub()
    q_stub.stubs(:subscribe).yields(header_stub, json)
    MQ.stubs(:queue).returns(q_stub)
    
    AmqpListener.load_listeners
    JsonListener.any_instance.expects(:on_message).with(hash)
    AmqpListener.run
  end
  
  def test_this_is_how_you_test_listeners_directly
    TestListener.side_effect = false
    TestListener.new.on_message("some message")
    assert_equal(true, TestListener.side_effect)
  end
  
  def test_this_is_how_you_test_sending_messages_directly
    @expectation = AmqpListener.expects(:send).with("q_name", "some message").once
    AmqpListener.send("q_name", "some message")
  end
  
  def test_another_way_to_test_messaging_listeners_directly
    TestListener.side_effect = false
    stub_sending_a_message(TestListener, "some message")
    assert_equal(true, TestListener.side_effect)    
  end
  
  def test_exception_handling
    TestListener.should_raise = true
    exceptions_handled = []
    AmqpListener.exception_handler do |listener, message, exception|
      exceptions_handled << [listener, message, exception]
    end
    stub_sending_a_message(TestListener, "message for exception handler test")
    assert_equal 1, exceptions_handled.size
    listener, message, exception = exceptions_handled[0]
    assert listener.is_a?(TestListener)
    assert_equal "message for exception handler test", message
    assert_equal "I'm raising", exception.message
  end
  
  def test_exception_notification_sending
    AmqpListener.use_default_exception_handler
    TestListener.should_raise = true
    begin
      require 'active_support'
      require 'active_record'
      ActiveRecord::Base
      require 'action_controller'
      require File.join(File.dirname(__FILE__), "..", "..", "exception_notification", "init")
    rescue LoadError => e
      puts "Can't run this test because couldn't load ExceptionNotifier: " + e.inspect
      return
    end    
    @email_to_deliver = false
    ExceptionNotifier.any_instance.expects(:perform_delivery_smtp).with do |email_body|
      @email_to_deliver = email_body
      true
    end.once
    TestListener.should_raise = true
    stub_sending_a_message(TestListener, "message for exception notification test")
    assert @email_to_deliver
    # puts @email_to_deliver.to_s
    [
      "message for exception notification test",
      "TestListener",
      "I'm raising"
    ].each do |expected|
      assert @email_to_deliver.to_s.index(expected), 
            "Expected to find '#{expected}' in body of email, but was #{@email_to_deliver.to_s}"
    end
  end
  
  private
  
  def stub_sending_a_message(listener_class, message_body)
    AmqpListener.stubs(:listeners).returns([listener_class])
    AMQP.stubs(:start).yields
    header_stub = stub(:ack => true)
    q_stub = stub()
    q_stub.stubs(:subscribe).yields(header_stub, message_body)
    MQ.stubs(:queue).returns(q_stub)
    AmqpListener.run
  end
  
end
