module Daybreak
  # HACK: Dangerous optimization on MRI which has a
  # global interpreter lock and makes the @queue array
  # thread safe.
  if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
    class Queue
      def initialize
        @queue, @full, @empty = [], [], []
        # Wake up threads 10 times per second to avoid deadlocks
        # since there is a race condition below
        @stop = false
        @heartbeat = Thread.new do
          until @stop
            unless @full.empty? || @empty.empty?
              @full.each(&:run)
              @empty.each(&:run)
            end
            sleep 0.1
          end
        end
      end

      def <<(x)
        @queue << x
        thread = @full.shift
        thread.run if thread
      end

      def pop
        @queue.shift
        if @queue.empty?
          thread = @empty.shift
          thread.run if thread
        end
      end

      def next
        while @queue.empty?
          begin
            # If a push happens here, the thread won't be woken up
            @full << Thread.current
            Thread.stop
          ensure
            @full.delete(Thread.current)
          end
        end
        @queue.first
      end

      def flush
        until @queue.empty?
          begin
            # If a pop happens here, the thread won't be woken up
            @empty << Thread.current
            Thread.stop
          ensure
            @empty.delete(Thread.current)
          end
        end
      end

      def stop
        @stop = true
        @heartbeat.join
      end
    end
  else
    class Queue
      def initialize
        @mutex = Mutex.new
        @full = ConditionVariable.new
        @empty = ConditionVariable.new
        @queue = []
      end

      def <<(x)
        @mutex.synchronize do
          @queue << x
          @full.signal
        end
      end

      def pop
        @mutex.synchronize do
          @queue.shift
          @empty.signal if @queue.empty?
        end
      end

      def next
        @mutex.synchronize do
          @full.wait(@mutex) while @queue.empty?
          @queue.first
        end
      end

      def flush
        @mutex.synchronize do
          @empty.wait(@mutex) until @queue.empty?
        end
      end

      def stop
      end
    end
  end
end
