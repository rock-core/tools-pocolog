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
require 'pocolog/block_stream'
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

    # Exception thrown when opening if the log file is not 
    #
    # One should run pocolog --upgrade-version when this happen
    class ObsoleteVersion < RuntimeError; end
    # Logfiles.open could not find a valid prologue in the provided file(s)
    #
    # This is most often because the provided file(s) are not pocolog files
    class MissingPrologue < RuntimeError; end
end

