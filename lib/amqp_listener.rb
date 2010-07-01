require 'rubygems'
#gem 'brontes3d-amqp'
require 'mq'
require 'bunny'

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
    def self.queue_options(queue_options)
      self.send(:define_method, :queue_options) do
        queue_options
      end
    end
    def self.subscribes_to(q_name)
      self.send(:define_method, :queue_name) do
        q_name
      end
    end
    def self.exchange_config(config)
      self.send(:define_method, :exchange_config) do
        config
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
  
  def self.expand_config(config_given)
    to_return = (symbolize_config(config_given) || {}).dup
    to_return[:fallback_servers] = (to_return[:fallback_servers] || []).dup
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
  
  cattr_accessor :log_time_stamp_format
  def self.log_time_stamp_format
    @@time_stamp_format ||= "%Y-%m-%d %H:%M:%S %Z"
  end
  def self.log_time_stamp
    "[#{Time.now.utc.strftime(AmqpListener.log_time_stamp_format)}]"
  end
  
  def self.get_exception_handler
    @@exception_handler ||= Proc.new do |listener, message, exception|
      if defined?(ExceptionNotifier)
        ExceptionNotifier.deliver_exception_notification(exception, nil, nil, 
                  {:info => {:listener => listener.class.name, :message => message}})
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
  
  def self.with_bunny(conf = expand_config(self.config), message = nil)
    begin
      bunny = Bunny.new(conf)
      bunny.start
      yield bunny
    rescue Exception, Timeout::Error => e
      get_exception_handler.call(bunny, message, e)
      if next_host = conf[:fallback_servers].shift
        conf[:host] = next_host[:host]
        retry
      else
        raise e
      end
    end
  end
  
  def self.send_to_exchange(routing_key, message, exchange_name = "amq.topic", exchange_type = :topic, exchange_opts = {}, message_opts = {})
    if Thread.current[:mq]
      exchange = MQ::Exchange.new(MQ.new, exchange_type.to_sym, exchange_name, exchange_opts)
      exchange.publish(message, {:routing_key => routing_key}.merge(message_opts))
    else
      # send synchronously with Bunny instead      
      with_bunny(expand_config(self.config), message) do |bunny|
        exchange = bunny.exchange(exchange_name, {:type => exchange_type.to_s}.merge(exchange_opts))
        exchange.publish(message, {:key => routing_key}.merge(message_opts))
      end
    end
  end
  
  def self.send(to_queue, message, reliable = true, q_opts = {}, message_opts = {}, bunny = nil)
    send_it = Proc.new do |q_maker|
      if reliable
        queue = q_maker.queue(to_queue, {:durable => true, :auto_delete => false}.merge(q_opts))
        queue.publish(message, {:persistent => true}.merge(message_opts))
      else
        queue = q_maker.queue(to_queue, {:durable => false, :auto_delete => false}.merge(q_opts))
        queue.publish(message, {:persistent => false}.merge(message_opts))
      end
    end
    
    if bunny
      send_it.call(bunny)
    elsif Thread.current[:mq]
      send_it.call(MQ)
    else
      # send synchronously with Bunny instead      
      with_bunny(expand_config(self.config), message) do |bunny|
        send_it.call(bunny)
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
  
  def self.start(base_config = self.config)
    expanded_config = expand_config(base_config)
    AMQP.start(expanded_config) do
      MQ.prefetch(1)
      @@running_config = expanded_config
      yield expanded_config
    end
  end
    
  def self.running_config
    @@running_config ||= {}
  end
  
  def self.run(base_config = self.config)
    Signal.trap('INT') { AMQP.stop{ EM.stop } }
    Signal.trap('TERM'){ AMQP.stop{ EM.stop } }
    
    load_listeners
    start(base_config) do |expanded_config|
      self.listeners.each do |l|
        listener = l.new
        
        unless listener.queue_name
          raise ArgumentError, "#{l} needs to specify the queue_name it subscribes_to"
        end
        
        bind_proc = nil
        info_string = "#{AmqpListener.log_time_stamp} registering listener #{l.inspect} on Q #{listener.queue_name.to_s.inspect}"
        if listener.respond_to?(:exchange_config) && listener.exchange_config
          exchange_options = listener.exchange_config[:exchange_options] || {}
          exchange_type = listener.exchange_config[:type] || :topic
          exchange_name = listener.exchange_config[:name] || 'amq.topic'
          bind_key = listener.exchange_config[:bind_key]
          unless bind_key
            raise ArgumentError, "Can't specify exchange_config without a :bind_key, got #{listener.exchange_config.inspect}"
          end
          exchange = MQ::Exchange.new( MQ.new, exchange_type, exchange_name, 
                                       {:durable => true, :auto_delete => false}.merge(exchange_options))
          bind_proc = Proc.new do |queue|
            queue.bind(exchange, {:key => bind_key}) 
          end
          info_string << " on a #{exchange_type} Exchange"
          info_string << " named #{exchange_name.inspect}"
          info_string << " bound with the key #{bind_key.inspect}"
        end
        AmqpListener.log :info, info_string
        
        extra_queue_opts = (listener.respond_to?(:queue_options) && listener.queue_options) || {}
        queue = MQ.queue(listener.queue_name, {:durable => true, :auto_delete => false}.merge(extra_queue_opts))
        
        if bind_proc
          bind_proc.call(queue)
        end
        
        queue.subscribe(:ack => true) do |h, m|
          run_message(listener, h, m)
        end
      end
    end
  end
  
  def self.run_message(listener, header, message)
    if AMQP.closing?
      AmqpListener.log :debug, "\n#{AmqpListener.log_time_stamp} #{message} (ignored, redelivered later)"
    else
      message_transformed = message
      retry_count = 0
      begin
        AmqpListener.log :info, "\n#{AmqpListener.log_time_stamp} #{listener} is handling message: #{message}"
        if listener.respond_to?(:exception_handler=)
          listener.exception_handler = Proc.new do |exception|
            get_exception_handler.call(listener, message_transformed, exception)
            header.ack
            AmqpListener.log :error, "#{AmqpListener.log_time_stamp} #{listener} got exception while handling #{message} -- #{exception}"
            AmqpListener.log :error, exception.backtrace.join("\n")
          end
        end
        if listener.respond_to?(:transform_message)
          message_transformed = listener.transform_message(message)
          listener.on_message(message_transformed)
        else
          listener.on_message(message)
        end
        header.ack
        AmqpListener.log :info, "#{AmqpListener.log_time_stamp} #{listener} done handling #{message}"
      rescue AmqpListener::AlreadyLocked => e
        #don't ack
        # header.reject(:requeue => true)
        AmqpListener.log :warn, "\n#{AmqpListener.log_time_stamp} #{listener} failed to get lock, will retry in 1 second #{message} -- #{e}"
        sleep(locks_config[:sleep_time])
        if retry_count >= locks_config[:max_retry]
          raise e
        else
          retry_count += 1
          retry
        end
      rescue => exception
        get_exception_handler.call(listener, message_transformed, exception)
        header.ack
        AmqpListener.log :error, "\n#{AmqpListener.log_time_stamp} #{listener} got exception while handling #{message} -- #{exception}"
        AmqpListener.log :error, exception.backtrace.join("\n")
      end
    end
  end
  
end