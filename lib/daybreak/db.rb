module Daybreak
  # Daybreak::DB contains the public api for Daybreak. It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  # @api public
  class DB
    include Enumerable

    # Set default value, can be a callable
    attr_writer :default

    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    # @param [String] file the path to the db file
    # @param [Hash] options a hash that contains the options for creating a new database
    # @option options [Class] :serializer Serializer class
    # @option options [Class] :format Format class
    # @option options [Object] :default Default value
    # @yield [key] a block that will return the default value to store.
    # @yieldparam [String] key the key to be stored.
    def initialize(file, options = {}, &block)
      @serializer = (options[:serializer] || Serializer::Default).new
      @table = Hash.new(&method(:hash_default))
      @journal = Journal.new(file, (options[:format] || Format).new, @serializer) do |record|
        if !record
          @table.clear
        elsif record.size == 1
          @table.delete(record.first)
        else
          @table[record.first] = @serializer.load(record.last)
        end
      end
      @default = block ? block : options[:default]
      @mutex = Mutex.new # Mutex used by #synchronize and #lock
      @@databases_mutex.synchronize { @@databases << self }
    end

    # Database file name
    # @return [String] database file name
    def file
      @journal.file
    end

    # Return default value belonging to key
    # @param [Object] key the default value to retrieve.
    # @return [Object] value the default value
    def default(key = nil)
      @table.default(@serializer.key_for(key))
    end

    # Retrieve a value at key from the database. If the default value was specified
    # when this database was created, that value will be set and returned. Aliased
    # as <tt>get</tt>.
    # @param [Object] key the value to retrieve from the database.
    # @return [Object] the value
    def [](key)
      @table[@serializer.key_for(key)]
    end
    alias_method :get, '[]'

    # Set a key in the database to be written at some future date. If the data
    # needs to be persisted immediately, call <tt>db.set!(key, value)</tt>.
    # @param [Object] key the key of the storage slot in the database
    # @param [Object] value the value to store
    # @return [Object] the value
    def []=(key, value)
      key = @serializer.key_for(key)
      @journal << [key, value]
      @table[key] = value
    end
    alias_method :set, '[]='

    # set! flushes data immediately to disk.
    # @param [Object] key the key of the storage slot in the database
    # @param [Object] value the value to store
    # @return [Object] the value
    def set!(key, value)
      set(key, value)
      flush
      value
    end

    # Delete a key from the database
    # @param [Object] key the key of the storage slot in the database
    # @return [Object] the value
    def delete(key)
      key = @serializer.key_for(key)
      @journal << [key]
      @table.delete(key)
    end

    # Immediately delete the key on disk.
    # @param [Object] key the key of the storage slot in the database
    # @return [Object] the value
    def delete!(key)
      value = delete(key)
      flush
      value
    end

    # Update database with hash (Fast batch update)
    # @param [Hash] hash the key/value hash
    # @return [DB] self
    def update(hash)
      shash = {}
      hash.each do |key, value|
        shash[@serializer.key_for(key)] = value
      end
      @journal << shash
      @table.update(shash)
      self
    end

    # Updata database and flush data to disk.
    # @param [Hash] hash the key/value hash
    # @return [DB] self
    def update!(hash)
      update(hash)
      @journal.flush
    end

    # Does this db have this key?
    # @param [Object] key the key to check if the DB has it
    # @return [Boolean]
    def has_key?(key)
      @table.has_key?(@serializer.key_for(key))
    end
    alias_method :key?, :has_key?
    alias_method :include?, :has_key?
    alias_method :member?, :has_key?

    # Does this db have this value?
    # @param [Object] value the value to check if the DB has it
    # @return [Boolean]
    def has_value?(value)
      @table.has_value?(value)
    end
    alias_method :value?, :has_value?

    # Return the number of stored items.
    # @return [Fixnum]
    def size
      @table.size
    end
    alias_method :length, :size

    # Utility method that will return the size of the database in bytes,
    # useful for determining when to compact
    # @return [Fixnum]
    def bytesize
      @journal.bytesize
    end

    # Counter of how many records are in the journal
    # @return [Fixnum]
    def logsize
      @journal.size
    end

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
    # @return [Array<String>]
    def keys
      @table.keys
    end

    # Flush all changes to disk.
    # @return [DB] self
    def flush
      @journal.flush
      self
    end

    # Sync the database with what is on disk, by first flushing changes, and
    # then loading the new records if necessary.
    # @return [DB] self
    def load
      @journal.load
      self
    end
    alias_method :sunrise, :load

    # Lock the database for an exclusive commit across processes and threads
    # @note This method performs an expensive locking over process boundaries.
    #       If you want to synchronize only between threads, use #synchronize.
    # @see #synchronize
    # @yield a block where every change to the database is synced
    # @yieldparam [DB] db
    # @return result of the block
    def lock
      synchronize { @journal.lock { yield self } }
    end

    # Synchronize access to the database from multiple threads
    # @note Daybreak is not thread safe, if you want to access it from
    #       multiple threads, all accesses have to be in the #synchronize block.
    # @see #lock
    # @yield a block where every change to the database is synced
    # @yieldparam [DB] db
    # @return result of the block
    def synchronize
      @mutex.synchronize { yield self }
    end

    # Remove all keys and values from the database.
    # @return [DB] self
    def clear
      @table.clear
      @journal.clear
      self
    end

    # Compact the database to remove stale commits and reduce the file size.
    # @return [DB] self
    def compact
      @journal.compact { @table }
      self
    end

    # Close the database for reading and writing.
    # @return nil
    def close
      @journal.close
      @@databases_mutex.synchronize { @@databases.delete(self) }
      nil
    end

    # Check to see if we've already closed the database.
    # @return [Boolean]
    def closed?
      @journal.closed?
    end

    private

    # @private
    @@databases = []

    # @private
    @@databases_mutex = Mutex.new

    # A handler that will ensure that databases are closed and synced when the
    # current process exits.
    # @private
    def self.exit_handler
      loop do
        db = @@databases_mutex.synchronize { @@databases.shift }
        break unless db
        warn "Daybreak database #{db.file} was not closed, state might be inconsistent"
        begin
          db.close
        rescue Exception => ex
          warn "Failed to close daybreak database: #{ex.message}"
        end
      end
    end

    at_exit { Daybreak::DB.exit_handler }

    # The block used in @table for new records
    def hash_default(_, key)
      if @default != nil
        value = @default.respond_to?(:call) ? @default.call(key) : @default
        @journal << [key, value]
        @table[key] = value
      end
    end
  end
end
