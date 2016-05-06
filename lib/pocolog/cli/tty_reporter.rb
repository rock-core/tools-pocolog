require 'pastel'
require 'tty-progressbar'

module Pocolog
    module CLI
        class TTYReporter
            attr_reader :progress_bar
            attr_reader :c_warn
            attr_reader :c_error
            def initialize(format, **options)
                @progress_bar = TTY::ProgressBar.new(format, **options)
                progress_bar.resize(60)
                pastel = Pastel.new
                @c_warn = pastel.yellow.detach
                @c_error = pastel.bright_red.detach
            end

            def log(msg)
                progress_bar.log(msg)
            end

            def advance(step = 1)
                progress_bar.advance(step)
            end

            def warn(msg)
                log(c_warn.(msg))
            end

            def error(msg)
                log(c_error.(msg))
            end

            def finish
                progress_bar.finish
            end
        end
    end
end

