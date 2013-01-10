module Daybreak
  class Serializer
    def key_for(key)
      key.to_s
    end

    def dump(value)
      Marshal.dump(value)
    end

    def load(value)
      Marshal.load(value)
    end
  end
end
