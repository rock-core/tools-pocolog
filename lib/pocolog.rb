require 'utilrb/time/to_hms'
require 'set'
require 'typelib'
require 'tempfile'
require 'stringio'
require 'zlib'

require 'pocolog/warn_deprecated'

require 'pocolog/convert'
require 'pocolog/format'
require 'pocolog/io_sequence'

require 'pocolog/block_stream'
require 'pocolog/data_stream'
require 'pocolog/sample_enumerator'
require 'pocolog/stream_aligner'
require 'pocolog/logfiles'
require 'pocolog/stream_info'
require 'pocolog/file_index_builder'
require 'pocolog/version'
require 'pocolog/stream_index'
require 'utilrb/pkgconfig'
require 'utilrb/logger'

module Pocolog
    # setup logger for Pocolog module
    extend Logger::Root('pocolog.rb', Logger::INFO)

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

require 'pocolog/file_upgrader'

