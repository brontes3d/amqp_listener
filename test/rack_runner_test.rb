require File.join(File.dirname(__FILE__), "test_helper")

class RackRunnerTest < ActiveSupport::TestCase
  
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
  
  class AppStub
    attr_accessor :callback
    def call(env)
      @callback.call
      true
    end
  end
  
  def test_rack_runner_when_we_get_a_response
    #This block stubs out event machine from making any calls to TCP..
    #causes the attempt to connect to "just succeeed"
    EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
      EM.next_tick do
        @client = EM.class_eval{ @conns }[99]
        @client.stubs(:send_data).returns(true)
        @client.succeed(@client)
      end
      true
    end
    
    @q_stub_got_messages = []
    
    #This block of stubbing captures attemps to send messages... 
    #and whenever it captures such a thing, it simulates sending a message to the response_q
    MQ::Queue.any_instance.stubs(:publish).with do |message, opts|
      @q_stub_got_messages << message
      decoded = ActiveSupport::JSON.decode(message)
      queue = MQ.queue(decoded["response_q"])
      queue.receive "fake header", "response to #{message}"
      true
    end.returns(true)
    
    @app_stub_got_responses = []
    
    #app stubs the actual Rack app that is handling a request
    #in this case all we want it to do is excercise AmqpListener::RackRunner.run
    app = AppStub.new
    app.callback = Proc.new do
      AmqpListener::RackRunner.run do |task|
        response_q_name = "named_response_q_100"
        queue = MQ.queue(response_q_name)
        queue.subscribe(:ack => false) do |h, m|
          @app_stub_got_responses << m
          task.done
        end
        AmqpListener.send("get_q", {:response_q => response_q_name}.to_json, false)
      end
    end
    
    #now that everything is set up, we invoke it
    env = stub()    
    rack_runner = AmqpListener::RackRunner.new(app)
    rack_runner.call(env)
    
    assert_equal(["{\"response_q\":\"named_response_q_100\"}"], @q_stub_got_messages)
    assert_equal(["response to {\"response_q\":\"named_response_q_100\"}"], @app_stub_got_responses)
  end

  def test_rack_runner_when_we_timeout
    #This block stubs out event machine from making any calls to TCP..
    #causes the attempt to connect to "just succeeed"
    EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
      EM.next_tick do
        @client = EM.class_eval{ @conns }[99]
        @client.stubs(:send_data).returns(true)
        @client.succeed(@client)
      end
      true
    end
    
    @q_stub_got_messages = []
    
    #This block of stubbing captures attemps to send messages... 
    #and whenever it captures such a thing, it simulates sending a message to the response_q
    MQ::Queue.any_instance.stubs(:publish).with do |message, opts|
      @q_stub_got_messages << message
      true
    end.returns(true)
    
    @app_stub_got_responses = []
    
    #app stubs the actual Rack app that is handling a request
    #in this case all we want it to do is excercise AmqpListener::RackRunner.run
    app = AppStub.new
    app.callback = Proc.new do
      AmqpListener::RackRunner.run do |task|
        response_q_name = "named_response_q_99"
        queue = MQ.queue(response_q_name)
        queue.subscribe(:ack => false) do |h, m|
          @app_stub_got_responses << m
          task.done
        end
        AmqpListener.send("get_q", {:response_q => response_q_name}.to_json, false)
      end
    end
    
    #now that everything is set up, we invoke it
    env = stub()    
    rack_runner = AmqpListener::RackRunner.new(app)
    rack_runner.call(env)
    #TODO: how do we assert that we waited 2 seconds before passing control back main thread here?
    
    assert_equal(["{\"response_q\":\"named_response_q_99\"}"], @q_stub_got_messages)
    assert_equal([], @app_stub_got_responses)
  end
  
  def test_multiple_tasks_at_once
    #This block stubs out event machine from making any calls to TCP..
    #causes the attempt to connect to "just succeeed"
    EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
      EM.next_tick do
        @client = EM.class_eval{ @conns }[99]
        @client.stubs(:send_data).returns(true)
        @client.succeed(@client)
      end
      true
    end
    
    @q_stub_got_messages = []
    
    #This block of stubbing captures attemps to send messages... 
    #and whenever it captures such a thing, it simulates sending a message to the response_q
    MQ::Queue.any_instance.stubs(:publish).with do |message, opts|
      @q_stub_got_messages << message
      decoded = ActiveSupport::JSON.decode(message)
      queue = MQ.queue(decoded["response_q"])
      queue.receive "fake header", "response to #{message}"
      true
    end.returns(true)
    
    @app_stub_got_responses = []
    
    #app stubs the actual Rack app that is handling a request
    #in this case all we want it to do is excercise AmqpListener::RackRunner.run
    app = AppStub.new
    app.callback = Proc.new do
      task1 = AmqpListener::RackRunner.prepare_task do |task|
        response_q_name = "named_response_q_98"
        queue = MQ.queue(response_q_name)
        queue.subscribe(:ack => false) do |h, m|
          @app_stub_got_responses << m
          task.done
        end
        AmqpListener.send("get_q", {:response_q => response_q_name}.to_json, false)
      end
      task2 = AmqpListener::RackRunner.prepare_task do |task|
        response_q_name = "named_response_q_97"
        queue = MQ.queue(response_q_name)
        queue.subscribe(:ack => false) do |h, m|
          @app_stub_got_responses << m
          task.done
        end
        AmqpListener.send("get_q", {:response_q => response_q_name}.to_json, false)
      end
      AmqpListener::RackRunner.run_tasks(task1, task2)
    end
    
    #now that everything is set up, we invoke it
    env = stub()    
    rack_runner = AmqpListener::RackRunner.new(app)
    rack_runner.call(env)
    
    assert_equal(["{\"response_q\":\"named_response_q_98\"}", "{\"response_q\":\"named_response_q_97\"}"], 
                 @q_stub_got_messages)
    assert_equal(["response to {\"response_q\":\"named_response_q_98\"}", "response to {\"response_q\":\"named_response_q_97\"}"], 
                 @app_stub_got_responses)
  end
  
  def test_send_from_main_thread
    #This block stubs out event machine from making any calls to TCP..
    #causes the attempt to connect to "just succeeed"
    EventMachine.stubs(:connect_server).returns(99).with do |arg1, arg2| 
      EM.next_tick do
        @client = EM.class_eval{ @conns }[99]
        @client.stubs(:send_data).returns(true)
        @client.succeed(@client)
      end
      true
    end
    
    @q_stub_got_messages = []
    
    #This block of stubbing captures attemps to send messages... 
    #and whenever it captures such a thing, it simulates sending a message to the response_q
    MQ::Queue.any_instance.stubs(:publish).with do |message, opts|
      @q_stub_got_messages << message
      true
    end.returns(true)
    
    app = AppStub.new
    app.callback = Proc.new do
      AmqpListener.send("test_send_q", "test_message_to_send_q")
    end
    
    #now that everything is set up, we invoke it
    env = stub()    
    rack_runner = AmqpListener::RackRunner.new(app)
    rack_runner.call(env)
    
    assert_equal(["test_message_to_send_q"], @q_stub_got_messages)
  end
  
end
