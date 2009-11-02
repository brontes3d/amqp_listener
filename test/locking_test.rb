require File.join(File.dirname(__FILE__), "test_helper")

require 'activerecord'
ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :database => ":memory:")

load File.expand_path(File.dirname(__FILE__) + "/mocks/schema.rb")
require File.expand_path(File.dirname(__FILE__) + '/mocks/lock.rb')

class LockingTest < ActiveSupport::TestCase
  
  def setup
    super
    AmqpListener.stubs(:get_exception_handler).returns(Proc.new{})
  end
  
  def test_locking
    assert !Lock.find_by_name("some name")
    Lock.with_lock("some name") do
      assert Lock.find_by_name("some name")
      assert_raises(ActiveRecord::StatementInvalid) do
        Lock.create!(:name => "some name")
      end
    end
    assert !Lock.find_by_name("some name")
  end
  
  def test_already_locked
    Lock.with_lock("some name") do
      assert_raises(AmqpListener::AlreadyLocked) do
        Lock.with_lock("some name") do
          #do nothing
        end
      end
    end
    assert_nothing_raised do
      Lock.with_lock("some name") do
        #do nothing
      end
    end
  end
  
  #test that if when processing a message there is a lock problem, we don't ack the message
  def test_we_retry_if_lock_fails_while_handling_message_and_then_dont_ack
    AmqpListener.load_listeners
    setup_test_listener_ack_counting_stubs(LockingListener)
    
    @try_count = 0
    
    LockingListener.any_instance.stubs(:on_message).with do |arg|
      @try_count += 1
      Lock.with_lock("some lock dont ack") do
        Lock.with_lock("some lock dont ack") do
        end
      end
    end
    
    assert_raises(AmqpListener::AlreadyLocked){
      AmqpListener.run      
    }
    assert_equal(0, @times_acked)
    assert_equal(6, @try_count) #config says max_retry is 5... so 1 initial try + 5 retries
  end
  
  def test_we_do_ack_if_other_failure
    AmqpListener.load_listeners
    setup_test_listener_ack_counting_stubs(LockingListener)
    
    LockingListener.any_instance.stubs(:on_message).with do
      raise "other failure"
    end
    
    AmqpListener.run
    assert_equal(1, @times_acked)
  end

  def test_we_do_ack_if_no_failure
    AmqpListener.load_listeners
    setup_test_listener_ack_counting_stubs(LockingListener)
        
    AmqpListener.run
    assert_equal(1, @times_acked)
  end
  
  #test multiple messages running at the same time... 
  # def test_locking_listeners_run_sequential
  #   AmqpListener.load_listeners
  #   q = get_message_q_simulator([LockingListener.new, 0.1], 
  #                               [LockingListener.new, 0.2], 
  #                               [LockingListener.new, 0.3])
  #   # l1 = LockingListener.new
  #   # l2 = LockingListener.new
  #   t1 = Thread.new do
  #     begin
  #       q.run
  #       # AmqpListener.run_message(l1, stub(), 3)
  #     rescue => e
  #       puts e.inspect
  #       puts e.backtrace.join("\n")
  #     end
  #   end
  #   t2 = Thread.new do
  #     begin
  #       q.run
  #       # AmqpListener.run_message(l2, stub(), 3)
  #     rescue => e
  #       puts e.inspect
  #       puts e.backtrace.join("\n")
  #     end
  #   end
  #   t1.join
  #   t2.join
  # end
  #without locks they can run at the same time... 
  #with locks they run sequential
  
  private
  
  def setup_test_listener_ack_counting_stubs(listner_class)
    @times_acked = 0
    AMQP.stubs(:start).yields
    header_stub = stub(:ack => true)
    header_stub.stubs(:ack).with{ @times_acked += 1 }
    qstub = stub()
    qstub.stubs(:subscribe).yields(header_stub, "message body")
    MQ.stubs(:queue).returns(qstub)
    AmqpListener.stubs(:listeners).returns([listner_class])
  end
  
  # def get_message_q_simulator(*messages_to_run)
  #   to_return = stub()
  #   to_return.stubs(:run).with do
  #     while !messages_to_run.empty?
  #       (l, m) = messages_to_run.pop
  #       acked = false
  #       header_stub = stub()
  #       header_stub.stubs(:ack).with do
  #         acked = true
  #       end
  #       AmqpListener.run_message(l, header_stub, m)
  #       unless acked
  #         sleep(0.5)
  #         messages_to_run.push([l,m])
  #       end
  #     end
  #   end
  #   to_return
  # end
  
end
