module Daybreak
  # Daybreak::DB contains the public api for Daybreak. It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  # @api public
  class DB
    include Enumerable

    # Accessors for the database file, and a counter of how many records are in
    # sync with the file.
    attr_reader :file, :logsize
    attr_writer :default

    @databases = []
    @databases_mutex = Mutex.new

    # A handler that will ensure that databases are closed and synced when the
    # current process exits.
    at_exit do
      loop do
        db = @databases_mutex.synchronize { @databases.first }
        break unless db
        warn "Daybreak database #{db.file} was not closed, state might be inconsistent"
        begin
          db.close
        rescue Exception => ex
          warn "Failed to close daybreak database: #{ex.message}"
        end
      end
    end

    class << self
      # @api private
      def register(db)
        @databases_mutex.synchronize { @databases << db }
      end

      # @api private
      def unregister(db)
        @databases_mutex.synchronize { @databases.delete(db) }
      end
    end

    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    # @param [String] file the path to the db file
    # @param [Hash] options a hash that contains the options for creating a new
    #  database. You can pass in :serializer, :format or :default.
    # @yield [key] a block that will return the default value to store.
    # @yieldparam [String] key the key to be stored.
    def initialize(file, options = {}, &block)
      @file = file
      @serializer = (options[:serializer] || Serializer::Default).new
      @format = (options[:format] || Format).new
      @default = block ? block : options[:default]
      @queue = Queue.new
      @table = {}
      open
      @mutex = Mutex.new # Mutex to make #lock thread safe
      @worker = Thread.new(&method(:worker))
      @worker.priority = -1
      load
      self.class.register(self)
    end

    # Return default value belonging to key
    # @param key the default value to retrieve.
    def default(key = nil)
      @default.respond_to?(:call) ? @default.call(key) : @default
    end

    # Retrieve a value at key from the database. If the default value was specified
    # when this database was created, that value will be set and returned. Aliased
    # as <tt>get</tt>.
    # @param key the value to retrieve from the database.
    def [](key)
      skey = @serializer.key_for(key)
      value = @table[skey]
      if value != nil || @table.has_key?(skey)
        value
      elsif @default
        value = default(key)
        @queue << [skey, value]
        @table[skey] = value
      end
    end
    alias_method :get, :'[]'

    # Set a key in the database to be written at some future date. If the data
    # needs to be persisted immediately, call <tt>db.set(key, value, true)</tt>.
    # @param [#to_s] key the key of the storage slot in the database
    # @param value the value to store
    def []=(key, value)
      key = @serializer.key_for(key)
      @queue << [key, value]
      @table[key] = value
    end
    alias_method :set, :'[]='

    # set! flushes data immediately to disk.
    # @param key the key of the storage slot in the database
    # @param value the value to store
    def set!(key, value)
      set(key, value)
      flush
      value
    end

    # Delete a key from the database
    # @param key the key of the storage slot in the database
    def delete(key)
      key = @serializer.key_for(key)
      @queue << [key]
      @table.delete(key)
    end

    # Immediately delete the key on disk.
    # @param key the key of the storage slot in the database
    def delete!(key)
      value = delete(key)
      flush
      value
    end

    # Update database with hash (Fast batch update)
    def update(hash)
      shash = {}
      hash.each do |key, value|
        shash[@serializer.key_for(key)] = value
      end
      @queue << shash
      @table.update(shash)
      self
    end

    # Updata database and flush data to disk.
    def update!(hash)
      update(hash)
      flush
    end

    # Does this db have a value for this key?
    # @param key the key to check if the DB has a key.
    def has_key?(key)
      @table.has_key?(@serializer.key_for(key))
    end
    alias_method :key?, :has_key?
    alias_method :include?, :has_key?
    alias_method :member?, :has_key?

    def has_value?(value)
      @table.has_value?(value)
    end
    alias_method :value?, :has_value?

    # Return the number of stored items.
    # @return [Integer]
    def size
      @table.size
    end
    alias_method :length, :size

    # Return true if database is empty.
    # @return [Boolean]
    def empty?
      @table.empty?
    end

    # Iterate over the key, value pairs in the database.
    # @yield [key, value] blk the iterator for each key value pair.
    # @yieldparam key the key.
    # @yieldparam value the value from the database.
    def each(&block)
      @table.each(&block)
    end

    # Return the keys in the db.
    # @return [Array]
    def keys
      @table.keys
    end

    # Flush all changes to disk.
    def flush
      @queue.flush
      self
    end

    # Sync the database with what is on disk, by first flushing changes, and
    # then reading the file if necessary.
    def sync
      flush
      load
    end

    # Lock the database for an exclusive commit accross processes and threads
    # @yield a block where every change to the database is synced
    def lock
      @mutex.synchronize do
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

    # Remove all keys and values from the database.
    def clear
      flush
      with_tmpfile do |path, file|
        file.write(@format.header)
        file.close
        # Clear acts like a compactification
        File.rename(path, @file)
      end
      @table.clear
      open
      self
    end

    # Compact the database to remove stale commits and reduce the file size.
    def compact
      sync
      with_tmpfile do |path, file|
        # Compactified database has the same size -> return
        return self if @pos == file.write(dump)
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

    # Close the database for reading and writing.
    def close
      @queue << nil
      @worker.join
      @fd.close
      @queue.stop if @queue.respond_to?(:stop)
      self.class.unregister(self)
      nil
    end

    # Check to see if we've already closed the database.
    def closed?
      @fd.closed?
    end

    private

    # Update the @table with records
    def load
      buf = read
      until buf.empty?
        record = @format.parse(buf)
        if record.size == 1
          @table.delete(record.first)
        else
          @table[record.first] = @serializer.load(record.last)
        end
        @logsize += 1
      end
      self
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
    def dump
      dump = @format.header
      # each is faster than inject
      @table.each do |record|
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
      dump = ''
      records.each do |record|
        record[1] = @serializer.dump(record.last)
        dump << @format.dump(record)
      end
      write(dump)
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
