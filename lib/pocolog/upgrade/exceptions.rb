module Pocolog
    module Upgrade
        class InvalidCast < ArgumentError; end

        # Exception raised when trying to automatically convert two arrays of
        # different sizes
        class ArraySizeMismatch < InvalidCast
        end
    end
end

