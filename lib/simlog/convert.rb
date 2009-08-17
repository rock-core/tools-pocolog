
module Pocosim
    class Logfiles
	def self.write_prologue(to_io, big_endian = nil)
	    to_io.write(MAGIC)
	    if big_endian.nil?
		big_endian = Pocosim.big_endian?
	    end
	    to_io.write(*[FORMAT_VERSION, big_endian ? 1 : 0].pack('xVV'))
	end

	def self.copy_block(block_info, from_io, to_io, buffer)
	    # copy the block as-is
	    from_io.seek(block_info.pos)
	    buffer = from_io.read(BLOCK_HEADER_SIZE + block_info.payload_size, buffer)
            if block_given?
                yield(buffer)
            end
	    to_io.write(buffer)
	end

	# Converts a version 1 logfile. Modifications:
	# * no prologue
	# * no compressed flag on data blocks
	# * time was written as [type, sec, usec, padding], with each
	#   field a 32-bit integer
	def self.from_version_1(from, to_io, big_endian)
	    write_prologue(to_io, big_endian)
	    from_io = from.rio

	    buffer = ""
	    uncompressed = [0].pack('C')
	    from.each_block(true, false) do |block_info|
		if block_info.type == STREAM_BLOCK
		    copy_block(block_info, from_io, to_io, buffer)
		elsif block_info.type == CONTROL_BLOCK
		    # remove the fields in time structure
		    to_io.write([block_info.type, block_info.index, block_info.payload_size - 16].pack('CxvV'))
		    from_io.seek(block_info.pos + BLOCK_HEADER_SIZE + 4)
		    to_io.write(from_io.read(8))
		    from_io.seek(4, IO::SEEK_CUR)
		    to_io.write(from_io.read(1))
		    from_io.seek(4, IO::SEEK_CUR)
		    to_io.write(from_io.read(8))
		else
		    size_offset = - 16 + 1

		    to_io.write([block_info.type, block_info.index, block_info.payload_size + size_offset].pack('CxvV'))
		    from_io.seek(block_info.pos + BLOCK_HEADER_SIZE + 4)
		    to_io.write(from_io.read(8))
		    from_io.seek(8, IO::SEEK_CUR)
		    to_io.write(from_io.read(8))
		    from_io.seek(4, IO::SEEK_CUR)
		    to_io.write(from_io.read(4))
		    to_io.write(uncompressed)
		    from_io.read(block_info.payload_size - (DATA_HEADER_SIZE - size_offset), buffer)
		    to_io.write(buffer)
		end
	    end
	end

	def self.to_new_format(from_io, to_io, big_endian = nil)
	    from = Logfiles.new(from_io)
	    from.read_prologue

	rescue MissingPrologue
	    # This is format version 1. Need either --little-endian or --big-endian
	    if big_endian.nil?
		raise "#{from_io.path} looks like a v1 log file. You must specify either --little-endian or --big-endian"
	    end
	    puts "#{from_io.path}: format v1 in #{big_endian ? "big endian" : "little endian"}"
	    from_version_1(from, to_io, big_endian)

	rescue ObsoleteVersion
	end
	
	def self.compress(from_io, to_io)
	    from = Logfiles.new(from_io)
	    from.read_prologue
	    write_prologue(to_io, from.endian_swap ^ Pocosim.big_endian?)

	    compressed = [1].pack('C')
	    buffer = ""
	    from.each_block(true) do |block_info|
		if block_info.type != DATA_BLOCK || block_info.payload_size < COMPRESSION_MIN_SIZE
		    copy_block(block_info, from_io, to_io, buffer)
		else
		    # Get the data header
		    data_header = from.data_header
		    if data_header.compressed
			copy_block(block_info, from_io, to_io, buffer)
			next
		    end

		    compressed = Zlib::Deflate.deflate(from.data)
		    delta = data_header.size - compressed.size
		    if Float(delta) / data_header.size > COMPRESSION_THRESHOLD
			# Save it in compressed form
			payload_size = DATA_HEADER_SIZE + compressed.size
			to_io.write([block_info.type, block_info.index, payload_size].pack('CxvV'))
			from_io.seek(block_info.pos + BLOCK_HEADER_SIZE)
			from_io.read(TIME_SIZE * 2, buffer)
			to_io.write(buffer << [compressed.size, 1].pack('VC'))
			to_io.write(compressed)
		    else
			copy_block(block_info, from_io, to_io, buffer)
		    end
		end
	    end
	end
    end
end

