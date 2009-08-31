class JsonListener < AmqpListener::Listener
  
  subscribes_to :test_json_q
  
  message_format :json_hash
  
  def on_message(message)
    #do nothing
  end
  
end