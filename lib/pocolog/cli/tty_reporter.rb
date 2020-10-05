# frozen_string_literal: true

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

            def initialize(format, colors: true, progress: true, **options)
                @base = 0
                @progress_enabled = progress
                reset_progressbar(format, **options)
                pastel = Pastel.new(enabled: colors)
                @c_warn = pastel.yellow.detach
                @c_info = pastel.yellow.detach
                @c_error = pastel.bright_red.detach
                @c_title = pastel.bold.detach
            end

            def reset_progressbar(format, **options)
                return unless @progress_enabled

                progress_bar.reset if @progress_bar
                @progress_bar = TTY::ProgressBar.new(format, **options)
                progress_bar.resize(60)
            end

            def log(msg)
                if progress_bar&.send(:tty?)
                    progress_bar.log(msg)
                else
                    $stdout.puts(msg)
                end
            end

            def current
                return unless @progress_enabled

                progress_bar.current - base
            end

            def current=(value)
                return unless @progress_enabled

                progress_bar.current = value + base
            end

            def advance(step = 1)
                return unless @progress_enabled

                progress_bar.advance(step)
            end

            def title(msg)
                log(c_title.call(msg))
            end

            def info(msg)
                log(c_info.call(msg))
            end

            def warn(msg)
                log(c_warn.call(msg))
            end

            def error(msg)
                log(c_error.call(msg))
            end

            def finish
                return unless @progress_enabled

                progress_bar.finish
            end
        end
    end
end
