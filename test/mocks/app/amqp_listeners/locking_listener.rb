class LockingListener < AmqpListener::Listener
  
  subscribes_to :test_locking
  
  def on_message(message)
    Lock.with_lock("LockingListener lock") do
      sleep(message.to_i)
    end
  end
  
end