module Daybreak
  # Daybreak::Journal handles background io, compaction and is the arbiter
  # of multiprocess safety
  # @api private
  class Journal
    attr_reader :logsize, :file

    def initialize(file, format, serializer, &blk)
      @file = file
      @format = format
      @serializer = serializer
      @queue = Queue.new
      @callback = blk
      open
      @worker = Thread.new(&method(:worker))
      @worker.priority = -1
      load
    end

    def closed?
      @fd.closed?
    end

    # Queue up a commit
    def <<(record)
      @queue << record
    end

    # Flush the commits to disk
    def flush
      @queue.flush
    end

    # Sync queued commits and read new commits from the log file
    def sync
      flush
      load
    end

    # Clear the queue and close the file handler
    def close
      @queue << nil
      @worker.join
      @fd.close
      @queue.stop if @queue.respond_to?(:stop)
    end

    # Lock the logfile across thread and process boundaries
    def lock
        # Flush everything to start with a clean state
        # and to protect the @locked variable
        flush

        with_flock(File::LOCK_EX) do
          load
          result = yield
          flush
          result
        end
      end
    end

    # Clear the database log and yield
    def clear
      flush
      with_tmpfile do |path, file|
        file.write(@format.header)
        file.close
        # Clear acts like a compactification
        File.rename(path, @file)
      end
      open
    end

    # Compact the logfile to represent the in-memory state
    def compact(records)
      sync
      with_tmpfile do |path, file|
        # Compactified database has the same size -> return
        return self if @pos == file.write(dump(records, @format.header))
        with_flock(File::LOCK_EX) do
          # Database was compactified in the meantime
          if @pos != nil
            # Append changed journal records if the database changed during compactification
            file.write(read)
            file.close
            File.rename(path, @file)
          end
        end
      end
      open
      load
    end

    # Emit records as we parse them
    def load
      buf = read
      until buf.empty?
        record = @format.parse(buf)
        emit(record)
        @logsize += 1
      end
    end

    def bytesize
      @fd.size
    end

    private

    def emit(record)
      @callback.call record
    end

    # Open or reopen file
    def open
      @fd.close if @fd
      @fd = File.open(@file, 'ab+')
      @fd.advise(:sequential) if @fd.respond_to? :advise
      stat = @fd.stat
      @inode = stat.ino
      @logsize = 0
      write(@format.header) if stat.size == 0
      @pos = nil
    end

    # Read new file content
    def read
      with_flock(File::LOCK_SH) do
        # File was opened
        unless @pos
          @fd.pos = 0
          @format.read_header(@fd)
        else
          @fd.pos = @pos
        end
        buf = @fd.read
        @pos = @fd.pos
        buf
      end
    end

    # Return database dump as string
    def dump(records, prefix = '')
      dump = prefix
      # each is faster than inject
      records.each do |record|
        record[1] = @serializer.dump(record.last)
        dump << @format.dump(record)
      end
      dump
    end

    # Worker thread
    def worker
      loop do
        case record = @queue.next
        when Hash
          write_batch(record)
        when nil
          @queue.pop
          break
        else
          write_record(record)
        end
        @queue.pop
      end
    rescue Exception => ex
      warn "Daybreak worker: #{ex.message}"
      retry
    end

    # Write batch update
    def write_batch(records)
      write(dump(records))
      @logsize += records.size
    end

    # Write single record
    def write_record(record)
      record[1] = @serializer.dump(record.last) if record.size > 1
      write(@format.dump(record))
      @logsize += 1
    end

    # Write data to output stream and advance @pos
    def write(dump)
      with_flock(File::LOCK_EX) do
        @fd.write(dump)
        # Flush to make sure the file is really updated
        @fd.flush
      end
      @pos = @fd.pos if @pos && @fd.pos == @pos + dump.bytesize
    end

    # Block with file lock
    def with_flock(mode)
      return yield if @locked
      begin
        loop do
          # HACK: JRuby returns false if the process is already hold by the same process
          # see https://github.com/jruby/jruby/issues/496
          Thread.pass until @fd.flock(mode)
          # Check if database was compactified in the meantime
          # break if not
          stat = @fd.stat
          break if stat.nlink > 0 && stat.ino == @inode
          open
        end
        @locked = true
        yield
      ensure
        @fd.flock(File::LOCK_UN)
        @locked = false
      end
    end

    # Open temporary file and pass it to the block
    def with_tmpfile
      path = [@file, $$.to_s(36), Thread.current.object_id.to_s(36)].join
      file = File.open(path, 'wb')
      yield(path, file)
    ensure
      file.close unless file.closed?
      File.unlink(path) if File.exists?(path)
    end
  end
end
