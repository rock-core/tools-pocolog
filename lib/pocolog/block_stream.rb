module Pocolog
    # Enumeration of blocks in a pocolog-compatible stream
    class BlockStream
        FORMAT_VERSION = Pocolog::Logfiles::FORMAT_VERSION

        # The size of the generic block header
	BLOCK_HEADER_SIZE = Logfiles::BLOCK_HEADER_SIZE
        # The size of a time in a block header
	TIME_SIZE = Logfiles::TIME_SIZE

        # The underlying IO
        attr_reader :io

        # Whether the data in the file is stored in little or big endian
        def big_endian?
            @big_endian
        end

        # Create a {BlockStream} object to sequentially interpret a stream of data
        #
        # @param [Boolean] tail if true, self will block waiting for new data
        #   when needed. Otherwise, it will assume the stream has been truncated
        def initialize(io, tail: false)
            @io  = io
            @tail = tail
            @big_endian = nil
            @native_endian = nil
            @payload_size = 0
            @buffer_io = StringIO.new
        end

        # Current position in {#io}
        def tell
            io.tell - (@buffer_io.size - @buffer_io.tell)
        end

        # Skip that many bytes in the stream
        def skip(count)
            buffer_remaining = (@buffer_io.size - @buffer_io.tell)
            if buffer_remaining < count
                @buffer_io.seek(buffer_remaining, IO::SEEK_CUR)
                io.seek(count - buffer_remaining, IO::SEEK_CUR)
            else
                @buffer_io.seek(count, IO::SEEK_CUR)
            end
            @payload_size -= count
        end

        # Whether the underlying IO is accessed in tail mode or not
        #
        # When in tail mode, self will block waiting for new data when
        # needed. Otherwise, it will assume the stream has been truncated
        def tail?
            @tail
        end

        # The IO path, if the backing IO is a file
        #
        # @return [String]
        def path
            io.path
        end

        # Whether this stream is closed
        #
        # @see close
        def closed?
            io.closed?
        end

        # Flush buffers to the underlying backing store
        def flush
            io.flush
        end

        # Close the file
        #
        # @see closed?
        def close
            io.close
        end

        # Magic code at the beginning of the log file
	MAGIC = "POCOSIM"

        # Read by 1MB chunks
        BUFFER_READ = 1024 * 1024

        # @api private
        #
        # Read bytes
        def read(size)
            if data = @buffer_io.read(size)
                remaining = (size - data.size)
            else
                remaining = size
            end
            if remaining > 0
                @buffer_io = StringIO.new(io.read([BUFFER_READ, remaining].max) || "")
                if buffer_data = @buffer_io.read(remaining) 
                    (data || "") + buffer_data
                else
                    data
                end
            else
                data
            end
        end

        # Write a pocolog file prologue in the given IO
	def self.write_prologue(io, big_endian: Pocolog.big_endian?)
	    io.write(MAGIC + [FORMAT_VERSION, big_endian ? 1 : 0].pack('xVV'))
	end

        # If the IO is a file, it starts with a prologue to describe the file
        # format
        #
        # Raises MissingPrologue if no prologue is found, or ObsoleteVersion if
        # the file format is not up-to-date (in which case one has to run
        # pocolog --to-new-format).
	def read_prologue # :nodoc:
	    header = read(MAGIC.size + 9) || ""
            magic = header[0, MAGIC.size]
            if magic != MAGIC
                if !magic
                    raise MissingPrologue, "#{io.path} is empty"
                else
                    raise MissingPrologue, "#{io.path} is not a pocolog log file. Got #{magic} at #{io.tell}, but was expecting #{MAGIC}"
                end
	    end

            format_version, big_endian = header[MAGIC.size, 9].unpack('xVV')

	    if format_version < FORMAT_VERSION
		raise ObsoleteVersion, "old format #{format_version}, current format is #{FORMAT_VERSION}. Convert it using the --to-new-format of pocolog"
	    elsif format_version > FORMAT_VERSION
		raise "this file is in v#{format_version} which is newer that the one we know #{FORMAT_VERSION}. Update pocolog"
	    end

            @format_version = format_version
            @big_endian = big_endian
            @native_endian = ((big_endian != 0) ^ Pocolog.big_endian?)
            @payload_size = 0
	end

        Block = Struct.new :kind, :stream_index, :payload_size, :raw_data

        # Interpret the next block
        def next
            if @payload_size != 0
                skip(@payload_size)
            end

            return if !(raw_header = read(Pocolog::Logfiles::BLOCK_HEADER_SIZE))
            if raw_header.size != Pocolog::Logfiles::BLOCK_HEADER_SIZE
                raise NotEnoughData, "not enought data while reading header at position the end of file"
            end

            type, index, payload_size = raw_header.unpack('CxvV')
            @payload_size = payload_size
            Block.new(type, index, payload_size, raw_header)
        end

        # Information about a stream declaration block
        class StreamBlock
            attr_reader :name
            attr_reader :typename
            attr_reader :registry_xml
            attr_reader :metadata_yaml

            def self.parse(raw_data)
                name_size = raw_data[1, 4].unpack('V').first
                name      = raw_data[5, name_size]
                typename_size = raw_data[5 + name_size, 4].unpack('V').first
                typename  = raw_data[9 + name_size, typename_size]

                offset = 9 + name_size + typename_size
                if raw_data.size > offset
                    registry_size = raw_data[offset, 4].unpack('V').first 
                    registry_xml = raw_data[offset + 4, registry_size]
                else
                    registry_xml = "<?xml version='1.0'?>\n<typelib></typelib>"
                end

                offset += 4 + registry_size
                if raw_data.size > offset
                    metadata_size = raw_data[offset, 4].unpack('V').first
                    metadata_yaml = raw_data[offset + 4, metadata_size]
                else
                    metadata_yaml = "--- {}\n"
                end

                new(name, typename, registry_xml, metadata_yaml)
            end

            def initialize(name, typename, registry_xml, metadata_yaml)
                @name, @typename, @registry_xml, @metadata_yaml =
                    name, typename, registry_xml, metadata_yaml

                @type = nil
                @metadata = nil
            end

            def valid_followup_stream?(other_stream)
                name == other_stream.name &&
                    type == other_stream.type &&
                    metadata == other_stream.metadata
            end

            def type
                if @type
                    @type
                else
                    registry = Typelib::Registry.from_xml(registry_xml)
                    @type = registry.get(typename)
                end
            end

            def metadata
                @metadata ||= YAML.load(metadata_yaml)
            end
        end

        # Read the payload of the last block returned by {#next}
        def read_payload(count = @payload_size)
            if count > @payload_size
                raise ArgumentError, "expected read count #{count} greater than remaining payload size #{@payload_size}"
            end

            result = read(count)
            if result.size != count
                raise NotEnoughData, "expected read #{@payload_size} but got #{result.size}"
            end

            @payload_size -= count
            result
        end

        def skip_payload
            skip(@payload_size)
        end

        # Information about a data block
        class DataBlockHeader
            attr_reader :rt_time
            attr_reader :lg_time
            attr_reader :data_size
            def compressed?; @compressed end

            SIZE = TIME_SIZE * 2 + 5

            def self.parse(raw_data)
		rt_sec, rt_usec, lg_sec, lg_usec, data_size, compressed =
                    raw_data.unpack('VVVVVC')
                new(rt_sec * 1_000_000 + rt_usec,
                    lg_sec * 1_000_000 + lg_usec,
                    data_size,
                    compressed)
            end

            def initialize(rt_time, lg_time, data_size, compressed)
                @rt_time = rt_time
                @lg_time = lg_time
                @data_size = data_size
                @compressed = compressed
            end
        end

        def read_data_block_header
            DataBlockHeader.parse(read_payload(DataBlockHeader::SIZE))
        end
    end
end

