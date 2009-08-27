require 'rubygems'
gem 'brontes3d-amqp'
require 'mq'

class AmqpListener
  
  class Listener    
    def self.subscribes_to(q_name)
      self.send(:define_method, :queue_name) do
        q_name
      end
    end
    def self.message_format(format)
      if format == :json_hash
        self.send(:define_method, :transform_message) do |message_body|
          ActiveSupport::JSON.decode(message_body)
        end
      else
        raise ArgumentError, "unknown format #{format}"
      end
    end
    def self.inherited(base)
      AmqpListener.listeners << base
    end
  end
  
  def self.config
    require 'activesupport'
    @@config ||= YAML.load_file("#{RAILS_ROOT}/config/amqp_listener.yml")
    @@configs ||= symbolized_config
  end
  
  def self.symbolized_config
    symbolize = nil
    symbolize_hash = Proc.new do |hash|
      hash.each do |k, v|
        hash[k.to_sym] = symbolize.call(v)
      end
    end
    symbolize_array = Proc.new do |array|
      array.collect do |v|
        symbolize.call(v)
      end
    end
    symbolize = Proc.new do |it|
      if it.is_a?(Hash)
        symbolize_hash.call(it)
      elsif it.is_a?(Array)
        symbolize_array.call(it)
      else
        it
      end
    end
    symbolize.call(@@config[RAILS_ENV].symbolize_keys)
  end
  
  
  def self.exception_handler(&block)
    @@exception_handler = block
  end
  def self.use_default_exception_handler
    @@exception_handler = nil
  end
  
  def self.get_exception_handler
    @@exception_handler ||= Proc.new do |listener, message, exception|
      if defined?(ExceptionNotifier)
        ExceptionNotifier.deliver_exception_notification(exception, nil, nil, 
                  {:info => 
                    {:listener => listener.class.name, :message => message}})
      else
        puts "Exception occured in #{listener} while handling message #{message} : " + exception.inspect
        puts exception.backtrace.join("\n")
      end
    end
  end
  
  def self.listeners
    @@listeners ||= []
    @@listeners
  end
  
  def self.listener_load_paths
    @@listener_load_paths ||= [default_load_path]
  end
  
  def self.default_load_path
    "#{RAILS_ROOT}/app/amqp_listeners/*.rb"
  end
  
  def self.send(to_queue, message)
    if Thread.current[:mq]
      queue = MQ.queue(to_queue, :durable => true)    
      queue.publish(message, {:persistent => true})      
    else
      begin
        AMQP.start(config) do
          queue = MQ.queue(to_queue, :durable => true)
      
          queue.publish(message, {:persistent => true})

          AMQP.stop do
            EM.stop
            #ALERT hacky workaround: 
            #Cause AMQP really shouldn't be doing @conn ||= connect *args
            #unless it's gonna reliably nullify @conn on disconnect (which is ain't)
            Thread.current[:mq] = nil
            AMQP.instance_eval{ @conn = nil }
            AMQP.instance_eval{ @closing = false }
          end
        end
      rescue RuntimeError => e
        if e.message == "no connection"
          retry
        else
          raise
        end
      end
    end
  end
  
  def self.load_listeners
    if self.listeners.empty?
      self.listener_load_paths.each do |load_path|
        Dir.glob(load_path).each { |f| require f }
      end
    end
  end
  
  def self.run(use_config = self.config)
    self.load_listeners
    
    Signal.trap('INT') { AMQP.stop{ EM.stop } }
    Signal.trap('TERM'){ AMQP.stop{ EM.stop } }
    
    AMQP.start(use_config) do
      self.listeners.each do |l|
        listener = l.new
        
        unless listener.queue_name
          raise "#{l} needs to specify the queue_name it subscribes_to"
        end
        puts "registering listener #{l.inspect} on Q #{listener.queue_name}"
        
        # NOTE : :auto_delete option is false by default
        queue = MQ.queue(listener.queue_name, :durable => true)
        
        queue.subscribe(:ack => true) do |h, m|
          if AMQP.closing?
            puts "#{m} (ignored, redelivered later)"
          else
            begin
              puts "#{listener} is handling message: #{m}"
              if listener.respond_to?(:transform_message)
                listener.on_message(listener.transform_message(m))
              else
                listener.on_message(m)
              end
            rescue => exception
              get_exception_handler.call(listener, m, exception)
            end
            #TODO: how do we know which exceptions to ack and which NOT to ack?
            puts "#{listener} done handling #{m}"
            h.ack
          end
        end

      end
    end
    
    
  end
  
end