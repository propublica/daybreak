module Daybreak
  class Writer
    def initialize(file)
      @fd = File.open file, 'a'
      @fd.binmode

      f = @fd.fcntl(Fcntl::F_GETFL, 0)
      @fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK | f)

      @worker = Worker.new(@fd)
    end

    def write(record)
      @worker.enqueue record
    end

    def finish!
      @worker.finish!
    end

    def flush!
      @worker.flush!
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
        buf = ""
        loop do
          str = @queue.pop
          if str.nil?
            @fd.flush
            break
          end
          buf << str
          read, write = IO.select [], [@fd]
          if write and fd = write.first
            lock(fd, File::LOCK_EX) { buf = try_write fd, buf }
          end
        end
      end

      def try_write(fd, buf)
        begin
          s = fd.write_nonblock(buf)
          if s < buf.length
            buf = buf[s..-1] # didn't finish
          else
            buf = ""
          end
        rescue Errno::EAGAIN
          buf = buf # try this again
        end
        buf
      end

      def flush!
        @queue.push nil
        @thread.join
        @thread = Thread.new { work }
      end

      def finish!
        @queue.push nil
        @thread.join
      end
    end
  end
end
