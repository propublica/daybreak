module Daybreak
  module Serializer
    # Default serializer which converts
    # keys to strings and marshalls values
    # @api public
    class Default
      def initialize
        @encoding = Encoding.find('ASCII-8BIT') if defined? Encoding
      end
      # Transform the key to a string
      # @param [Object] key
      # @return [String] key transformed to string
      def key_for(key)
        key = key.to_s
        key.force_encoding(@encoding) if @encoding && key.encoding != @encoding
        key
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
