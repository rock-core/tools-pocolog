require 'pocolog/format/v2'

module Pocolog
    class InvalidIndex < RuntimeError; end
    class MissingIndexPrologue < InvalidIndex; end
    class ObsoleteIndexVersion < InvalidIndex; end

    class InvalidFile < RuntimeError; end
    class MissingPrologue < InvalidFile; end
    class ObsoleteVersion < InvalidFile; end

    module Format
        Current = V2
    end
end
