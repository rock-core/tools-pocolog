module LogTools
    module Upgrade
        module Ops
            # Converts to an array
            class ArrayCast < Base
                attr_reader :element_ops

                def initialize(to_type, element_ops)
                    super(to_type)
                    @element_ops = element_ops
                end

                def call(target, value)
                    if value.size != to_type.length
                        raise ArraySizeMismatch, "attempting to copy a container of size #{value.size} into an array of size #{to_type.length}"
                    end

                    i = 0
                    value.raw_each do |sample|
                        element_ops.call(target.raw_get(i), sample)
                        i += 1
                    end
                end
            end
        end
    end
end

