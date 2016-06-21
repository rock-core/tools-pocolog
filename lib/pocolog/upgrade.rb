require 'rgl/adjacency'
require 'rgl/dijkstra'

require 'pocolog/upgrade/exceptions'
require 'pocolog/upgrade/converter_registry'

require 'pocolog/upgrade/ops/base'
require 'pocolog/upgrade/ops/identity'
require 'pocolog/upgrade/ops/numeric_cast'
require 'pocolog/upgrade/ops/array_cast'
require 'pocolog/upgrade/ops/container_cast'
require 'pocolog/upgrade/ops/enum_cast'
require 'pocolog/upgrade/ops/compound_cast'
require 'pocolog/upgrade/ops/sequence'
require 'pocolog/upgrade/ops/custom'

module Pocolog
    module Upgrade
        def self.compute(time, from_type, to_type, registered_converters, relax: false)
            chain, failures = registered_converters.find_converter_chain(time, from_type, to_type, relax: relax)
            if !chain
                raise NoChain.new(from_type, to_type, failures)
            elsif chain.empty?
                Ops::Identity.new(to_type)
            elsif chain.size == 1
                chain.first
            else
                Ops::Sequence.new(chain, to_type)
            end
        end

        def self.build_deep_cast(time, from_type, to_type, registered_converters, relax: false)
            if from_type < Typelib::NumericType
                if !(to_type < Typelib::NumericType)
                    raise InvalidCast, "cannot automatically cast a numeric type into a non-numeric type"
                end
                Ops::NumericCast.new(from_type, to_type)
            elsif from_type < Typelib::ArrayType || from_type < Typelib::ContainerType
                if to_type < Typelib::ArrayType
                    if from_type < Typelib::ArrayType
                        if from_type.length != to_type.length
                            raise ArraySizeMismatch, "cannot convert between arrays of different sizes"
                        end
                    end
                    element_conversion =
                        compute(time, from_type.deference, to_type.deference, registered_converters, relax: relax)
                    Ops::ArrayCast.new(to_type, element_conversion)

                elsif to_type < Typelib::ContainerType
                    # Can convert to arrays or containers
                    element_conversion =
                        compute(time, from_type.deference, to_type.deference, registered_converters)
                    Ops::ContainerCast.new(to_type, element_conversion)
                else
                    raise InvalidCast, "cannot automatically cast an array/container to a non-array/container"
                end
            elsif from_type < Typelib::EnumType
                if !(to_type < Typelib::EnumType)
                    raise InvalidCast, "cannot automatically cast an enum to a non-enum"
                end
                Ops::EnumCast.new(from_type, to_type)
            elsif from_type < Typelib::CompoundType
                if !(to_type < Typelib::CompoundType)
                    raise InvalidCast, "cannot automatically cast a compound to a non-compound"
                end
                field_convertions = Array.new

                from_type.each_field do |field_name, field_type|
                    if to_type.has_field?(field_name)
                        begin
                            field_ops = compute(time, field_type, to_type[field_name], registered_converters, relax: true)
                            field_convertions << [field_name, field_ops]
                        rescue InvalidCast
                            raise if !relax
                        end
                    end
                end

                if !relax
                    to_type.each_field do |field_name, field_type|
                        if !from_type.has_field?(field_name) && !(field_type <= Typelib::ContainerType)
                            raise CannotAddNonContainerField.new(to_type, field_name), "cannot automatically convert to a compound that adds a non-container field, #{to_type.name} adds #{field_name} of type #{field_type.name}"
                        end
                    end
                end
                Ops::CompoundCast.new(field_convertions, to_type)
            end
        end
    end
end

