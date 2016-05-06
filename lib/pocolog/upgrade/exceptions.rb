module Pocolog
    module Upgrade
        class InvalidCast < ArgumentError; end

        # Exception raised when trying to automatically convert two arrays of
        # different sizes
        class ArraySizeMismatch < InvalidCast
        end

        # Exception raised when a conversion chain cannot be found
        class NoChain < InvalidCast
        end
    end
end

