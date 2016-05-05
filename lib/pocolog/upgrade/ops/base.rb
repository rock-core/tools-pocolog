module LogTools
    module Upgrade
        module Ops
            # Base class for all the operations
            #
            # One MUST reimplement either {#call} or {#convert}. Otherwise, they
            # will do an infinite recursion
            class Base
                attr_reader :target_t

                def initialize(target_t)
                    @target_t = target_t
                end

                def call(target, value)
                    Typelib.copy(target, convert(value))
                end

                def convert(value)
                    target = target_t.new
                    call(target, value)
                    target
                end
            end
        end
    end
end

