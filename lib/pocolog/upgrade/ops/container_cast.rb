module LogTools
    module Upgrade
        module Ops
            class ContainerCast < Base
                attr_reader :element_ops

                def initialize(target_t, element_ops)
                    super(target_t)
                    @element_ops = element_ops
                end

                def call(target, value)
                    value.raw_each do |sample|
                        target.push(element_ops.convert(sample))
                    end
                end
            end
        end
    end
end
