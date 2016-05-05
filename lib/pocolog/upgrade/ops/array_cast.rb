module LogTools
    module Upgrade
        module Ops
            # Converts to an array
            class ArrayCast < Base
                attr_reader :element_ops

                def initialize(target_t, element_ops)
                    super(target_t)
                    @element_ops = element_ops
                end

                def call(target, value)
                    if value.size != target_t.length
                        raise ArraySizeMismatch, "attempting to copy a container of size #{value.size} into an array of size #{target_t.length}"
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

