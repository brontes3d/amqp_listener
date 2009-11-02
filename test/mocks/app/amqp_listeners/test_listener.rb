class TestListener < AmqpListener::Listener
  
  cattr_accessor :side_effect
  cattr_accessor :should_raise
  
  subscribes_to :test_q
  
  def on_message(message)
    if TestListener.should_raise
      raise "I'm raising"
    end
    TestListener.side_effect = true
  end
    
end