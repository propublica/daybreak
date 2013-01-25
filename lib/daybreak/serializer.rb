module Daybreak
  module Serializer
    # Serializer which only encodes key in binary
    # @api public
    class None
      # (see Daybreak::Serializer::Default#key_for)
      if ''.respond_to? :force_encoding
        def key_for(key)
          if key.encoding != Encoding::BINARY
            key = key.dup if key.frozen?
            key.force_encoding(Encoding::BINARY)
          end
          key
        end
      else
        def key_for(key)
          key
        end
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

    # Default serializer which converts
    # keys to strings and marshalls values
    # @api public
    class Default < None
      # Transform the key to a string
      # @param [Object] key
      # @return [String] key transformed to string
      def key_for(key)
        super(key.to_s)
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
  end
end
