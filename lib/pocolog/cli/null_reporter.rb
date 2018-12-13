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
            def reset_progressbar(format, **options)
            end
            def log(msg)
            end
            def advance(value)
                @current += value
            end
            def title(msg)
            end
            def info(msg)
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
