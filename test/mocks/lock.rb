class Lock < ActiveRecord::Base
  
  include AmqpListener::LockProvider
  
end
