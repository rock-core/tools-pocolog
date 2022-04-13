# frozen_string_literal: true

require 'pocolog/format/v2'

module Pocolog
    class InvalidIndex < RuntimeError; end
    class MissingIndexPrologue < InvalidIndex; end
    class ObsoleteIndexVersion < InvalidIndex; end

    class InvalidFile < RuntimeError; end
    class MissingPrologue < InvalidFile; end
    class ObsoleteVersion < InvalidFile; end

    # true if this machine is big endian
    def self.big_endian?
        "LAAS".unpack('L').pack('N') == "LAAS"
    end

    STREAM_BLOCK           = 1
    DATA_BLOCK             = 2
    CONTROL_BLOCK          = 3
    BLOCK_TYPES            = [STREAM_BLOCK, DATA_BLOCK, CONTROL_BLOCK]

    DATA_STREAM            = 1

    CONTROL_SET_TIMEBASE   = 0
    CONTROL_SET_TIMEOFFSET = 1

    module Format
        Current = V2
    end
end
