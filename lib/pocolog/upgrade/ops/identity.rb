module Pocolog
    module Upgrade
        module Ops
            class Identity < Base
                def convert(value)
                    value.cast(@to_type)
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
