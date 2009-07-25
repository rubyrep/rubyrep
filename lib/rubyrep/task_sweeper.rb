module RR

  # Monitors and cancels stalled tasks
  class TaskSweeper

    # Executes the give block in a separate Thread.
    # Returns if block is finished or stalled.
    # The block must call regular #ping to announce it is not stalled.
    # * +timeout_period+:
    #   Maximum time (in seonds) without ping, after which a task is considered stalled.
    # Returns the created sweeper (allows checking if task was terminated).
    def self.timeout(timeout_period)
      sweeper = TaskSweeper.new(timeout_period)
      sweeper.send(:timeout) {yield sweeper}
      sweeper
    end

    # Time in seconds after which a task is considered stalled. Timer is reset
    # by calling #ping.
    attr_accessor :timeout_period

    # Must be called by the executed task to announce it is still alive
    def ping
      self.last_ping = Time.now
    end

    # Returns +true+ if the task was timed out.
    # The terminated task is expected to free all resources and exit.
    def terminated?
      terminated
    end

    # Waits without timeout till the task executing thread is finished
    def join
      thread && thread.join
    end

    # Creates a new TaskSweeper
    # * +timeout_period+: timeout value in seconds
    def initialize(timeout_period)
      self.timeout_period = timeout_period
      self.terminated = false
      self.last_ping = Time.now
    end

    protected

    # Time of last ping
    attr_accessor :last_ping

    # Set to +true+ if the executed task has timed out
    attr_accessor :terminated

    # The task executing thread
    attr_accessor :thread

    # Executes the given block and times it out if stalled.
    def timeout
      exception = nil
      self.thread = Thread.new do
        begin
          yield
        rescue Exception => e
          # save exception so it can be rethrown outside of the thread
          exception = e
        end
      end
      while self.thread.join(self.timeout_period) == nil do
        if self.last_ping < Time.now - self.timeout_period
          self.terminated = true
          break
        end
      end
      raise exception if exception
    end
  end
end