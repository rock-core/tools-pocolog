require 'log_tools/upgrade/exceptions'
require 'log_tools/upgrade/custom_converter'
require 'log_tools/upgrade/converter_registry'

require 'log_tools/upgrade/ops/base'
require 'log_tools/upgrade/ops/identity'
require 'log_tools/upgrade/ops/numeric_cast'
require 'log_tools/upgrade/ops/array_cast'
require 'log_tools/upgrade/ops/container_cast'
require 'log_tools/upgrade/ops/enum_cast'
require 'log_tools/upgrade/ops/compound_cast'
require 'log_tools/upgrade/ops/sequence'

module Pocolog
    module Upgrade
        def self.compute(time, from_type, to_type, registered_converters)
            matching_converters = registered_converters.find_all_for(time, from_type)

            # Converters are only defined by their time limit. We run them
            # in time order as we can assume that a converter for "later"
            # samples assumes that its sample has been upgraded by "earlier"
            # converters
            matching_converters = matching_converters.sort_by(&:time_to)

            current_time = time
            current_type = from_type
            converters = Array.new
            matching_converters.each do |c|
                if c.applies_on?(current_time, current_type)
                    converters << c
                    current_time = c.time_to
                    current_type = c.to_type
                end
            end

            if current_type != to_type
                converters.concat(build_deep_cast(current_time, current_type, to_type, registered_converters))
            end
            if converters.empty?
                Ops::Identity.new(to_type)
            elsif converters.size == 1
                converters.first
            else
                Ops::Sequence.new(converters, to_type)
            end
        end

        def self.build_deep_cast(time, from_type, to_type, registered_converters)
            if from_type < Typelib::NumericType
                if !(to_type < Typelib::NumericType)
                    raise InvalidCast, "cannot automatically cast a numeric type into a non-numeric type"
                end
                [Ops::NumericCast.new(from_type, to_type)]
            elsif from_type < Typelib::ArrayType || from_type < Typelib::ContainerType
                if to_type < Typelib::ArrayType
                    if from_type < Typelib::ArrayType
                        if from_type.length != to_type.length
                            raise ArraySizeMismatch, "cannot convert between arrays of different sizes"
                        end
                    end
                    element_conversion =
                        compute(time, from_type.deference, to_type.deference, registered_converters)
                    [Ops::ArrayCast.new(to_type, element_conversion)]

                elsif to_type < Typelib::ContainerType
                    # Can convert to arrays or containers
                    element_conversion =
                        compute(time, from_type.deference, to_type.deference, registered_converters)
                    [Ops::ContainerCast.new(to_type, element_conversion)]
                else
                    raise InvalidCast, "cannot automatically cast an array/container to a non-array/container"
                end
            elsif from_type < Typelib::EnumType
                if !(to_type < Typelib::EnumType)
                    raise InvalidCast, "cannot automatically cast an enum to a non-enum"
                end
                [Ops::EnumCast.new(from_type, to_type)]
            elsif from_type < Typelib::CompoundType
                if !(to_type < Typelib::CompoundType)
                    raise InvalidCast, "cannot automatically cast a compound to a non-compound"
                end
                field_convertions = Array.new
                from_type.each_field do |field_name, field_type|
                    if to_type.has_field?(field_name)
                        field_ops = compute(time, field_type, to_type[field_name], registered_converters)
                        field_convertions << [field_name, field_ops]
                    end
                end
                to_type.each_field do |field_name, field_type|
                    if !from_type.has_field?(field_name) && !(field_type <= Typelib::ContainerType)
                        raise InvalidCast, "cannot automatically convert to a compound that adds a non-container field"
                    end
                end
                [Ops::CompoundCast.new(field_convertions, to_type)]
            end
        end
    end
end

