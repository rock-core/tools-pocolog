module Pocolog
    module Upgrade
        # Class that holds a set of custom convertion
        #
        # It allows to somewhat efficiently query which converters are available
        # for a given (time, type) pair through {#find_all_for}
        class ConverterRegistry
            # The converters indexed by their {CustomConverter#from_type}
            attr_reader :converter_by_from_type

            # The converters source types indexed by name
            attr_reader :from_type_by_type_name

            def initialize
                @converter_by_from_type = Hash.new { |h, k| h[k] = Array.new }
                @from_type_by_type_name = Hash.new { |h, k| h[k] = Set.new }
            end

            # Add a custom converter to this registry
            def add(custom_converter)
                # Check whether there is a known type with the same definition
                #
                # This is meant to reduce the type spent resolving types
                if from_type = find_equivalent_type(custom_converter.from_type)
                    custom_converter.from_type = from_type
                end
                if to_type = find_equivalent_type(custom_converter.to_type)
                    custom_converter.to_type = to_type
                end

                from_type = custom_converter.from_type
                converter_by_from_type[from_type] << custom_converter
                from_type_by_type_name[from_type.name] << from_type
            end
            
            # Find all converters applicable to a given type
            def find_all_for_type(type)
                if converters = converter_by_from_type[type]
                    return converters
                end

                if types = from_type_by_type_name.fetch(type.name, nil)
                    matching_types = types.find_all { |t| t == type }
                    matching_types.inject(Array.new) do |converters, t|
                        converters.concat(converter_by_from_type[t])
                    end
                else Array.new
                end
            end

            # Find all converters applicable to a time and type
            def find_all_for(time, type)
                find_all_for_type(type).find_all { |c| c.time_to >= time }
            end
        end
    end
end

