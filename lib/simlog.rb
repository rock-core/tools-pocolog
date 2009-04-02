require 'utilrb/time/to_hms'
require 'set'
require 'typelib'
require 'tempfile'
require 'stringio'
require 'zlib'

require 'simlog/convert'
require 'simlog/data_reader'
require 'simlog/data_writer'
require 'simlog/file'
require 'simlog/version'
require 'utilrb/pkgconfig'

module Pocosim
    # true if this machine is big endian
    def self.big_endian?
	"LAAS".unpack('L').pack('N') == "LAAS"
    end

    POCOSIM_TLB = "POCOSIM_DATA_DIR/pocosim.tlb"
    STREAM_BLOCK           = 1
    DATA_BLOCK             = 2
    CONTROL_BLOCK          = 3
    BLOCK_TYPES            = [STREAM_BLOCK, DATA_BLOCK, CONTROL_BLOCK]

    DATA_STREAM            = 1

    CONTROL_SET_TIMEBASE   = 0
    CONTROL_SET_TIMEOFFSET = 1

    def self.pocosim_tlb
	if defined? @pocosim_tlb
	    return @pocosim_tlb
	end

	@pocosim_tlb = nil
	pocosim = begin 
		      Utilrb::PkgConfig.new('pocosim')
		  rescue Utilrb::PkgConfig::NotFound
		      return
		  end

	path = File.join(pocosim.datadir, 'pocosim.tlb')
	if File.readable?(path)
	    @pocosim_tlb = path
	end
    end

    def self.load_tlb(registry)
	if tlb = pocosim_tlb
	    registry.import(tlb, 'tlb')
	end
    end
end

