require 'rubygems'
#gem 'brontes3d-amqp'
require 'mq'

class AmqpListener
  
  class AlreadyLocked < StandardError; end
  
  module LockProvider

    def self.included(base)
      base.extend ClassMethods
    end
    
    module ClassMethods
      def with_locks(*names)
        (first, *rest) = names
        unless first
          raise ArgumentError, "need to supply at least 1 lock name"
        end
        with_lock(first) do
          if rest.empty?
            yield
          else
            with_locks(*rest) do
              yield
            end
          end
        end
      end
      def with_lock(name)
        AmqpListener.log :info, "getting lock #{name}"
        lock = nil
        begin
          lock = self.create!(:name => name)
          AmqpListener.log :info, "got lock #{name}"
        rescue ActiveRecord::StatementInvalid => e
          AmqpListener.log :warn, "failed lock #{name}"
          raise AlreadyLocked, "unable to get lock named: #{name} (#{e.inspect})"
        end
        yield
      ensure
        begin
          lock.destroy if lock
        rescue ActiveRecord::StatementInvalid => e
          AmqpListener.log :warn, "failed to remove lock #{name}. (#{e.inspect}). retrying"
          retry          
        end
      end
    end
    
  end
  
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
    @@config ||= YAML.load_file("#{RAILS_ROOT}/config/amqp_listener.yml")
    @@configs ||= @@config[RAILS_ENV]
  end
  
  def self.locks_config
    @@locks_config ||= self.config[:lock] || {}
    @@locks_config[:sleep_time] ||= 1
    @@locks_config[:max_retry] ||= 15
    @@locks_config
  end
  
  def self.symbolize_config(c)
    symbolize = nil
    symbolize_hash = Proc.new do |hash|
      hash.each do |k, v|
        hash.delete(k)
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
    symbolize.call(c)
  end
  
  def self.expand_config(cofig_given)
    to_return = symbolize_config(cofig_given)
    if to_return[:host].is_a?(Array)
      to_return[:fallback_servers] ||= []
      to_return[:host], *rest = to_return[:host]
      rest.each do |host|
        to_append = {:host => host}
        if to_return[:port]
          to_append[:port] = to_return[:port]
        end
        to_return[:fallback_servers] << to_append
      end
    end
    to_return
  end
  
  def self.exception_handler(&block)
    @@exception_handler = block
  end
  def self.use_default_exception_handler
    @@exception_handler = nil
  end
  
  cattr_accessor :logger
  def self.log(level, string)
    if self.logger
      self.logger.call(level, string)
    else
      puts string
    end
  end
  def self.set_logger(&block)
    self.logger = block
  end
  
  def self.get_exception_handler
    @@exception_handler ||= Proc.new do |listener, message, exception|
      if defined?(ExceptionNotifier)
        ExceptionNotifier.deliver_exception_notification(exception, nil, nil, 
                  {:info => 
                    {:listener => listener.class.name, :message => message}})
      else
        AmqpListener.log :error, "Exception occured in #{listener} while handling message #{message} : " + exception.inspect
        AmqpListener.log :error, exception.backtrace.join("\n")
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
  
  def self.send(to_queue, message, reliable = true)
    send_it = Proc.new do
      if reliable
        queue = MQ.queue(to_queue, :durable => true)  
        queue.publish(message, {:persistent => true})
      else
        queue = MQ.queue(to_queue)
        queue.publish(message, {})
      end
    end
    if Thread.current[:mq]
      send_it.call
    else
      AMQP.start(expand_config(config)) do
        send_it.call
        shutdown
      end
    end
  end
  
  def self.shutdown
    AMQP.stop do
      EM.stop
      cleanup
    end
  end
  
  def self.cleanup
    #ALERT hacky workaround: 
    #Cause AMQP really shouldn't be doing @conn ||= connect *args
    #unless it's gonna reliably nullify @conn on disconnect (which is ain't)
    Thread.current[:mq] = nil
    AMQP.instance_eval{ @conn = nil }
    AMQP.instance_eval{ @closing = false }    
  end
  
  def self.load_listeners
    if self.listeners.empty?
      self.listener_load_paths.each do |load_path|
        Dir.glob(load_path).each { |f| require f }
      end
    end
  end
  
  def self.start(use_config = self.config)    
    AMQP.start(expand_config(use_config)) do
      yield
    end
  end
  
  def self.run(use_config = self.config)
    Signal.trap('INT') { AMQP.stop{ EM.stop } }
    Signal.trap('TERM'){ AMQP.stop{ EM.stop } }
    
    load_listeners
    start(use_config) do
      self.listeners.each do |l|
        listener = l.new
        
        unless listener.queue_name
          raise "#{l} needs to specify the queue_name it subscribes_to"
        end
        AmqpListener.log :info, "registering listener #{l.inspect} on Q #{listener.queue_name}"
        
        # NOTE : :auto_delete option is false by default
        queue = MQ.queue(listener.queue_name, :durable => true)
        
        queue.subscribe(:ack => true) do |h, m|
          run_message(listener, h, m)
        end
      end
    end
  end
  
  def self.run_message(listener, header, message)
    if AMQP.closing?
      AmqpListener.log :debug, "#{message} (ignored, redelivered later)"
    else
      retry_count = 0
      begin
        AmqpListener.log :info, "#{listener} is handling message: #{message}"
        if listener.respond_to?(:transform_message)
          listener.on_message(listener.transform_message(message))
        else
          listener.on_message(message)
        end
        header.ack
        AmqpListener.log :info, "#{listener} done handling #{message}"
      rescue AmqpListener::AlreadyLocked => e
        #don't ack
        # header.reject(:requeue => true)
        AmqpListener.log :warn, "#{listener} failed to get lock, will retry in 1 second #{message} -- #{e}"
        sleep(locks_config[:sleep_time])
        if retry_count >= locks_config[:max_retry]
          raise e
        else
          retry_count += 1
          retry
        end
      rescue => exception
        get_exception_handler.call(listener, message, exception)
        header.ack
        AmqpListener.log :error, "#{listener} got exception while handling #{message} -- #{exception}"
        AmqpListener.log :error, exception.backtrace.join("\n")
      end
    end    
  end
  
end