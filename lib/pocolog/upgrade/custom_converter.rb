module Pocolog
    module Upgrade
        # Definition of a custom conversion of a given type to another
        class CustomConverter
            # @return [String] the descriptive name of the converter
            attr_reader :name

            # @return [Time] the validity limit for this converter
            #   Samples created after this time won't be valid. Leave nil if
            #   this converter is purely based on type signatures.
            attr_reader :time_to

            # @return [Typelib::Type] the expected type signature of
            #   the value that will be converted
            attr_accessor :from_type

            # @return [Typelib::Type] the type signature of the 
            #   the value that has been converted
            attr_accessor :to_type

            # @return [#call] the object that will perform the conversion
            attr_reader :converter

            # Create a type converter for a given type, with an optional time
            # limit
            #
            # @param [String] name the descriptive name of the converter
            # @param [Time] time_to the validity limit for this converter
            #   Samples created after this time won't be valid. Leave nil if
            #   this converter is purely based on type signatures.
            # @param [Typelib::Type] from_type the expected type signature of
            #   the value that will be converted
            # @param [Typelib::Type] to_type the type signature of the 
            #   the value that has been converted
            def initialize(name, time_to, from_type, to_type, converter)
                @name = name
                @time_to = time_to
                @from_type = from_type
                @to_type   = to_type
                @converter = converter
            end

            def convert(sample)
                target_sample = to_type.new
                converter.call(target_sample, sample)
            end
        end
    end
end


