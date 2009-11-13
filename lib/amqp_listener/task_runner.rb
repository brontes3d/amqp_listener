class AmqpListener::TaskRunner
  
  class Task
    def initialize(&action)
      @done = false
      @action = action
    end
    def on_finish(&block)
      @finish_blocks ||= []
      @finish_blocks << block
    end
    def run
      @action.call(self)
    end
    def done?
      @done
    end
    def done(&block)
      unless @done
        @done = true
        @finish_blocks.each(&:call)
        if block_given?
          yield
        end
      end
    end
  end
  
  def self.run(&block)
    run_tasks(prepare_task(&block))
  end
  
  def self.prepare_task(&block)
    task = Task.new(&block)
    task.on_finish do
      @@tasks.delete(task)
    end
    task
  end
  
  def self.run_tasks(*tasks)
    @@tasks = ThreadSafeArray.new
    @@run_q = []
    tasks.each do |task|
      @@run_q << task
      @@tasks << task
    end
    if Thread.current[:mq]
      AmqpListener::TaskRunner.run_work_loop
    else
      begin
        current_thread = Thread.current
        Thread.new do
          @@tasks.each do |task|
            task.on_finish do
              if @@tasks.empty?
                AmqpListener.shutdown
                current_thread.run
              end
            end
          end
          AmqpListener.start do |config|
            AmqpListener::TaskRunner.run_work_loop
          end
        end
        unless @@tasks.empty?
          Thread.stop
        end
      rescue => e
        AmqpListener.log :error, e.inspect
        AmqpListener.log :error, e.backtrace.join("\n")
      end
    end
  end
  
  def self.run_work_loop
    @@task_timeout = (AmqpListener.running_config[:task_timeout] || 2).to_i
    loop do
      if @@run_q.empty?
        return
      else
        task = @@run_q.shift
        task.run
        EM.add_timer(@@task_timeout) do
          unless task.done?
            AmqpListener.log :warn, "Timed out on task #{task}"
            task.done
          end
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
  
end