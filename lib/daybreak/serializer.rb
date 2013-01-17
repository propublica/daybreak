module Daybreak
  module Serializer
    # Default serializer which converts
    # keys to strings and marshalls values
    # @api public
    class Default
      # Transform the key to a string
      # @param [Object] key
      # @return [String] key transformed to string
      def key_for(key)
        key.to_s
      end

      # Serialize a value
      # @param [Object] value
      # @return [String] value transformed to string
      def dump(value)
        Marshal.dump(value)
      end

      # Parse a value
      # @param [String] value
      # @return [Object] deserialized value
      def load(value)
        Marshal.load(value)
      end
    end

    # Serializer which does nothing
    # @api public
    class None
      # (see Daybreak::Serializer::Default#key_for)
      def key_for(key)
        key
      end

      # (see Daybreak::Serializer::Default#dump)
      def dump(value)
        value
      end

      # (see Daybreak::Serializer::Default#load)
      def load(value)
        value
      end
    end
  end
end
