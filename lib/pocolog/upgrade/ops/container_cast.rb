module Pocolog
    module Upgrade
        module Ops
            class ContainerCast < Base
                attr_reader :element_ops

                def initialize(to_type, element_ops)
                    super(to_type)
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
