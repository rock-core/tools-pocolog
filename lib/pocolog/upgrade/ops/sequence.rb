module LogTools
    module Upgrade
        module Ops
            # A class that applies a list of operations in sequence
            class Sequence < Base
                # The operations to apply, along with their intermdiate values
                attr_reader :ops

                def initialize(ops, target_t)
                    super(target_t)
                    @ops = ops.map do |op|
                        [op, op.target_t.new]
                    end
                end

                def convert(value)
                    ops.inject(value) do |v, (op, op_value)|
                        op.call(op_value, v)
                        op_value
                    end
                end
            end
        end
    end
end

