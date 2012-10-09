module Falconer
  class DB
    include Enumerable

    def initialize(file, default=nil)
      @file_name = file
      reset!
      @default   = default
      read_all!
    end

    def []=(key, value, sync = false)
      key = key.to_s
      @writer.write(Record.new(key, serialize(value)))
      flush! if sync
      @table[key] = value
    end
    alias_method :set, :"[]="

    def [](key)
      key = key.to_s
      if @table.has_key? key
        @table[key]
      elsif default?
        set key, @default
      end
    end
    alias_method :get, :"[]"

    def each(&blk)
      keys.each { |k| blk.call(k, get(k)) }
    end

    def default?
      !@default.nil?
    end

    def has_key?(key)
      @table.has_key? key.to_s
    end

    def keys
      @table.keys
    end

    def length
      @table.keys.length
    end

    def serialize(value)
      Marshal.dump(value)
    end

    def parse(value)
      Marshal.load(value)
    end

    def empty!
      reset!
      @writer.truncate!
    end

    def flush!
      @writer.finish!
    end

    def reset!
      @table  = {}
      @writer = Falconer::Writer.new(@file_name)
    end

    def close!
      @writer.close!
    end

    def compact!
      tmp_file = Tempfile.new File.basename(@file_name)
      copy_db  = DB.new tmp_file.path

      each do |key, i|
        copy_db.set(key, get(key))
      end
      copy_db.close!

      empty!

      tmp_file.close
      FileUtils.mv tmp_file.path, @file_name
      tmp_file.unlink

      close!
      reset!
      read_all!
    end

    def read_all!
      Reader.new(@file_name).read do |record|
        @table[record.key] = parse record.data
      end
    end
  end
end
