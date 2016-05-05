module LogTools
    module Upgrade
        module Ops
            # Casts a field in a compound
            class CompoundCast < Base
                attr_reader :field_convertions

                def initialize(field_convertions, target_t)
                    @field_convertions = field_convertions
                    super(target_t)
                end

                def call(target, value)
                    field_convertions.each do |field_name, field_ops|
                        field_ops.call(target.raw_get(field_name), value.raw_get(field_name))
                    end
                end
            end
        end
    end
end

