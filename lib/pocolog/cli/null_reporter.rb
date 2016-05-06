module Pocolog
    module CLI
        # A null object compatible with TTY::ProgressBar
        class NullReporter
            def log(msg)
            end
            def advance(*step)
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
