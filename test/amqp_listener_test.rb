require File.join(File.dirname(__FILE__), "test_helper")

require 'activerecord'
ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :database => ":memory:")

load File.expand_path(File.dirname(__FILE__) + "/mocks/schema.rb")
require File.expand_path(File.dirname(__FILE__) + '/mocks/lock.rb')

class AmqpListenerTest < ActiveSupport::TestCase
  
  def setup
    AmqpListener.load_listeners
    TestListener.should_raise = false
    TestListener.side_effect = false
  end
  
  def teardown
    AmqpListener.cleanup
    Thread.current[:mq] = nil
    AMQP.instance_eval{ @conn = nil }
    AMQP.instance_eval{ @closing = false }
    AMQP::Client.class_eval{ @retry_count = 0 }
    AMQP::Client.class_eval{ @server_to_select = 0 }    
  end
  
  def test_recieve_a_message
    AMQP.stubs(:start).yields
    header_stub = stub(:ack => true)
    q_stub = stub()
    q_stub.stubs(:subscribe).yields(header_stub, "message body")
    MQ.stubs(:queue).returns(q_stub)
    MQ.stubs(:prefetch).returns(true)
    
    AmqpListener.load_listeners
    TestListener.any_instance.expects(:on_message).with("message body")
    AmqpListener.run
  end
  
  def test_send_a_message
    # AMQP.stubs(:start).yields
    # q_stub = stub()
    # MQ.expects(:queue).with("test_q", :durable => true).returns(q_stub)
    # q_stub.expects(:publish).with("test message", :persistent => true)
    
    bunny_stub = stub()
    Bunny.stubs(:new).returns(bunny_stub)
    bunny_stub.stubs(:start).returns(true)

    q_stub = stub()
    bunny_stub.expects(:queue).with("test_q", :durable => true, :auto_delete => false).returns(q_stub)
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
    MQ.stubs(:prefetch).returns(true)
    
    AmqpListener.load_listeners
    JsonListener.any_instance.expects(:on_message).with(hash)
    AmqpListener.run
  end
  
  # def test_reconnect_logging
  #   class << RAILS_DEFAULT_LOGGER
  #     attr_accessor :messages_logged      
  #     def add(*args, &block)
  #       message = args[2]
  #       self.messages_logged << message
  #       super
  #     end
  #   end
  #   RAILS_DEFAULT_LOGGER.messages_logged = []
  #   
  #   disconnector = Proc.new do
  #     EM.next_tick do
  #       @client = EM.class_eval{ @conns }[99]
  #       @client.stubs(:send_data).returns(true)
  #       @client.connection_completed
  #       EM.class_eval{ @conns.delete(99) }
  #       @client.unbind
  #     end
  #   end
  #   
  #   EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
  #     disconnector.call
  #     true
  #   end
  #   
  #   #this test should continously connect and disconnect until this 1 second timer forces it to stop 
  #   EM.next_tick{ EM.add_timer(0.5){ EM.stop_event_loop } }
  #   AmqpListener.run
  #   
  #   assert_equal(["Connecting to nonexistant1 12345 (attempt 1)", 
  #                 "Connecting to nonexistant2 5672 (attempt 2)", 
  #                 "Connecting to nonexistanthost 5672 (attempt 3)"],
  #                 RAILS_DEFAULT_LOGGER.messages_logged)
  # end
  
  def test_expand_config
    simple = YAML::load %Q{
      test:
        host: [nonexistanthost, alsononexistant]
        port: 1234
        logging: true
        reconnect_timer: 0.1
    }
    expanded = YAML::load %Q{
      test:
        host: nonexistanthost
        port: 1234
        logging: true
        reconnect_timer: 0.1      
        fallback_servers:
          - host: alsononexistant
            port: 1234
    }
    assert_equal(AmqpListener.expand_config(simple['test']),  AmqpListener.symbolize_config(expanded['test']))
  end
  
  def test_reconnect_to_fallback_servers
    @times_connected = 0
    @connect_args = []
    
    disconnector = Proc.new do
      EM.next_tick do
        @client = EM.class_eval{ @conns }[99]
        @client.stubs(:send_data).returns(true)
        @client.connection_completed
        EM.class_eval{ @conns.delete(99) }
        @client.unbind
      end
    end
    
    EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
      @connect_args << [arg1, arg2]
      @times_connected += 1
      disconnector.call
      true
    end
    
    #this test should continously connect and disconnect until this 1 second timer forces it to stop 
    EM.next_tick{ EM.add_timer(1){ EM.stop_event_loop } }
    AmqpListener.run
    # puts "reconnected #{@times_connected} times"
    # puts "connect_args #{@connect_args.inspect}"
    assert_equal(7, @times_connected)
    assert_equal([
      ["nonexistanthost", 5672], ["nonexistant1", 12345], ["nonexistant2", 5672], 
      ["nonexistanthost", 5672], ["nonexistant1", 12345], ["nonexistant2", 5672], 
      ["nonexistanthost", 5672]],
      @connect_args)
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
    assert_equal([:test_q, {:durable => true, :auto_delete => false}], @queue_created_with_args)
  end
  
  def test_non_durable_listener
    stub_sending_a_message(NonDurable, "some message")
    assert_equal([:non_durable_q, {:durable => false, :auto_delete => true}], @queue_created_with_args)    
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
    MQ.stubs(:queue).returns(q_stub).with do |qname, options|
      @queue_created_with_args = [qname, options]
      true
    end
    MQ.stubs(:prefetch).returns(true)
    AmqpListener.run
  end
  
end
