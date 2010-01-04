class NonDurable < AmqpListener::Listener
    
  subscribes_to :non_durable_q
  
  queue_options :durable => false, :auto_delete => true
  
  def on_message(message)
    #do nothing
  end
  
end