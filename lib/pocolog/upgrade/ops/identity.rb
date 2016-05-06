module LogTools
    module Upgrade
        module Ops
            class Identity < Base
                def convert(value)
                    value
                end

                def call(target, value)
                    Typelib.copy(target, value)
                end

                def identity?
                    true
                end
            end
        end
    end
end
