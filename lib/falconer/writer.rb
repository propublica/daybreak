module Falconer
  class Writer
    def initialize(file)
      @fd = File.open file, 'a'
      @fd.binmode
      @fd.sync = true
      @worker = Worker.new(@fd)
    end

    def write(record)
      @worker.enqueue record
    end

    def finish!
      @worker.finish!
    end

    def close!
      finish!
      @fd.close
    end

    def truncate!
      @fd.truncate(0)
    end

    private

    class Worker
      include Locking

      def initialize(fd)
        @queue  = Queue.new
        @fd     = fd
        @buffer = ""
        @thread = Thread.new { work }
        at_exit { finish! }
      end

      def enqueue(record)
        @queue << record.representation
      end

      def work
        str = ""
        loop do
          str = @queue.pop
          if str.nil?
            @fd.flush
            break
          end
          read, write = IO.select [], [@fd]
          if write and fd = write.first
            lock @fd, File::LOCK_EX do
              @fd.write(str)
            end
          end
        end
      end

      def finish!
        @queue.push nil
        @thread.join
      end
    end
  end
end
