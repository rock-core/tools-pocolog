module Pocolog
    module Upgrade
        class InvalidCast < ArgumentError; end

        # Exception raised when trying to automatically convert two arrays of
        # different sizes
        class ArraySizeMismatch < InvalidCast
        end

        # Exception raised when a conversion chain cannot be found
        class NoChain < InvalidCast
            attr_reader :from_type
            attr_reader :to_type
            attr_reader :failures

            def initialize(from_type, to_type, failures)
                @from_type = from_type
                @to_type   = to_type
                @failures  = failures
            end

            def pretty_print(pp)
                if from_type.name == to_type.name
                    pp.text "no upgrade chain for #{from_type.name}"
                else
                    pp.text "no conversion chain from #{from_type.name} to #{to_type.name}"
                end

                failures.each do |e|
                    pp.nest(2) do
                        pp.breakable
                        e.pretty_print(pp)
                    end
                end
            end
        end

        class CannotAddNonContainerField < InvalidCast
            attr_reader :type
            attr_reader :field_name

            def initialize(type, field_name)
                @type = type
                @field_name = field_name
            end

            def pretty_print(pp)
                pp.text "cannot automatically convert if the new type adds a non-container field"
                pp.breakable
                pp.text "#{type.name} adds #{field_name}"
                pp.breakable
                pp.nest(2) do
                    pp.breakable
                    type.pretty_print(pp)
                end
            end
        end
    end
end

