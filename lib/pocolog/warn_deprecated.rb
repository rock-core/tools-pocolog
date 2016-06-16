module Pocolog
    def self.show_deprecations?
        @show_deprecations
    end

    def self.show_deprecations=(flag)
        @show_deprecations = flag
    end

    @show_deprecations = true

    # Warns about a deprecation, showing the first backtrace lines
    def self.warn_deprecated(message)
        return if !show_deprecations?

        # Show the message, regardless of the actual logger setup
        current_logger_level = logger.level
        logger.level = Logger::WARN

        warn message
        caller(1)[0, 4].each do |line|
            warn "  #{line}"
        end

    ensure
        if current_logger_level
            logger.level = current_logger_level
        end
    end
end
