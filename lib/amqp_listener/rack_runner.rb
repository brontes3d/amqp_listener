class AmqpListener::RackRunner
  
  class Task
    def initialize(response_thread, &action)
      @done = false
      @response_thread = response_thread
      @action = action
    end
    def on_finish(&block)
      @finish_block = block
    end
    def run
      @action.call(self)
    end
    def done
      unless @done
        @done = true
        @finish_block.call(@response_thread)
      end
    end
  end
  
  def self.enabled?
    defined?(@@amqp_thread) && @@amqp_thread
  end
  
  def self.run(&block)
    run_tasks(prepare_task(&block))
  end
  
  def self.prepare_task(&block)
    task = Task.new(Thread.current, &block)
    task.on_finish do |response_thread|
      @@tasks.delete(task)
      if @@tasks.empty?
        response_thread.run
      end
      run_work_loop unless @@work_loop_running
    end
    task
  end
  
  def self.run_tasks(*tasks)
    tasks.each do |task|
      @@run_q << task
      @@tasks << task
    end
    @@amqp_thread.run
    unless @@tasks.empty?
      Thread.stop
    end
  end
  
  def self.run_work_loop
    @@work_loop_running = true
    while(true) do
      if @@run_q.empty?
        if @@tasks.empty?
          Thread.stop
        else
          @@work_loop_running = false
          return
        end
      else
        task = @@run_q.shift
        task.run
        #TODO: make this timeout configurable
        EM.add_timer(@@task_timeout) do
          AmqpListener.log :warn, "Timed out on task #{task}"
          task.done
        end
      end
    end
  end
  
  class ThreadSafeArray < Array
    def initialize
      @mutex = Mutex.new
      super
    end
    def <<(arg)
      @mutex.synchronize{ super }
    end
    def shift
      @mutex.synchronize{ super }
    end
    def empty?
      @mutex.synchronize{ super }
    end
  end
  
  def initialize app, options = {}
    @app = app
  end
  #each passenger proc should have it's OWN amqp_thread.. <-- test TODO
  def call env    
    @@tasks ||= ThreadSafeArray.new
    @@run_q ||= []
    @@amqp_thread ||= Thread.new do
      begin
        AmqpListener.start do |config|
          @@task_timeout = (config[:task_timeout] || 2).to_i
          AmqpListener::RackRunner.run_work_loop
        end
      rescue => e
        AmqpListener.log :error, e.inspect
        AmqpListener.log :error, e.backtrace.join("\n")
      end
    end
    @app.call(env)
  end
end