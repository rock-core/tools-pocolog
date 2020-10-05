module Pocolog
    module Upgrade
        module Ops
            # A class that applies a list of operations in sequence
            class Sequence < Base
                # The operations to apply, along with their intermdiate values
                attr_reader :ops

                def initialize(ops, to_type)
                    super(to_type)
                    @ops = ops.map do |op|
                        [op, op.to_type.new]
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

