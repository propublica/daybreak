module Daybreak
  # Writer's handle the actually fiddly task of committing data to disk.
  # They have a Worker instance that writes in a select loop.
  class Writer
    # Open up the file, ready it for binary and nonblocking writing.
    def initialize(file)
      @fd = File.open file, 'a'
      @fd.binmode

      f = @fd.fcntl(Fcntl::F_GETFL, 0)
      @fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK | f)

      @worker = Worker.new(@fd)
    end

    # Send a record to the workers queue.
    def write(record)
      @worker.enqueue record
    end

    # Finish writing
    def finish!
      @worker.finish!
    end

    # Flush pending commits, and restart the worker.
    def flush!
      @worker.flush!
    end

    # Finish writing and close the file descriptor
    def close!
      finish!
      @fd.close
    end

    # Truncate the file.
    def truncate!
      @fd.truncate(0)
    end

    private

    # Workers handle the actual fiddly bits of asynchronous io and
    # and handle background writes.
    class Worker
      include Locking

      def initialize(fd)
        @queue  = Queue.new
        @fd     = fd
        @buffer = ""
        @thread = Thread.new { work }
        at_exit { finish! }
      end

      # Queue up a write to be committed later.
      def enqueue(record)
        @queue << record.representation
      end

      # Loop and block if we don't have work to do or if
      # the file isn't ready for another write just yet.
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

      # Try and write the buffer to the file via non blocking file writes.
      # If the write fails try again.
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

      # finish! and start up another worker thread.
      def flush!
        finish!
        @thread = Thread.new { work }
        true
      end

      # Push a nil through the queue and block until the write loop is finished.
      def finish!
        @queue.push nil
        @thread.join
      end
    end
  end
end
