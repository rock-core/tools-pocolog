module LogTools
    module Upgrade
        module Ops
            # Base class for all the operations
            #
            # One MUST reimplement either {#call} or {#convert}. Otherwise, they
            # will do an infinite recursion
            class Base
                attr_reader :to_type

                def initialize(to_type)
                    @to_type = to_type
                end

                def call(target, value)
                    Typelib.copy(target, convert(value))
                end

                def convert(value)
                    target = to_type.new
                    call(target, value)
                    target
                end

                def identity?
                    false
                end
            end
        end
    end
end

