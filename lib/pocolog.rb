require 'utilrb/time/to_hms'
require 'set'
require 'typelib'
require 'tempfile'
require 'stringio'
require 'zlib'

require 'pocolog/convert'
require 'pocolog/data_reader'
require 'pocolog/stream_aligner'
require 'pocolog/data_writer'
require 'pocolog/file'
require 'pocolog/version'
require 'utilrb/pkgconfig'

module Pocolog
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
end

