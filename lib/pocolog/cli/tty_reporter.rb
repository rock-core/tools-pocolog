require 'pastel'
require 'tty-progressbar'

module Pocolog
    module CLI
        class TTYReporter
            attr_reader :progress_bar
            attr_reader :c_warn
            attr_reader :c_error
            attr_reader :c_title
            attr_reader :c_info

            # Base value for {#current}
            #
            # It works as an offset between the reporter's current value and the
            # underlying progress handler
            attr_accessor :base

            def initialize(format, **options)
                @base = 0
                @progress_bar = TTY::ProgressBar.new(format, **options)
                progress_bar.resize(60)
                pastel = Pastel.new
                @c_warn = pastel.yellow.detach
                @c_info = pastel.yellow.detach
                @c_error = pastel.bright_red.detach
                @c_title = pastel.bold.detach
            end

            def log(msg)
                progress_bar.log(msg)
            end

            def current
                progress_bar.current - base
            end

            def current=(value)
                progress_bar.current = value + base
            end

            def advance(step = 1)
                progress_bar.advance(step)
            end

            def title(msg)
                log(c_title.(msg))
            end

            def info(msg)
                log(c_info.(msg))
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

