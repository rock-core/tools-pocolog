module Pocolog
    module CLI
        # A null object compatible with TTY::ProgressBar
        class NullReporter
            attr_accessor :base
            attr_accessor :current
            def initialize
                @current = 0
                @base = 0
            end
            def log(msg)
            end
            def advance(value)
                @current += value
            end
            def warn(msg)
            end
            def error(msg)
            end
            def finish
            end
        end
    end
end
