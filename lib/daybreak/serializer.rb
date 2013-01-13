module Daybreak
  module Serializer
    # Default serializer which converts
    # keys to strings and marshalls values
    # @api public
    class Default
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

    # Serializer which does nothing
    # @api public
    class None
      def key_for(key)
        key
      end

      def dump(value)
        value
      end

      def load(value)
        value
      end
    end
  end
end
