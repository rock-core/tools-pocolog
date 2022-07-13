# frozen_string_literal: true

require "pocolog/format"
require "pocolog/stream_index"

module Pocolog
    # Enumeration of blocks in a pocolog-compatible stream
    class BlockStream
        FORMAT_VERSION = Format::Current::VERSION

        # The size of the generic block header
        BLOCK_HEADER_SIZE = Format::Current::BLOCK_HEADER_SIZE
        # The size of a time in a block header
        TIME_SIZE = Format::Current::TIME_SIZE

        # Magic code at the beginning of the log file
        MAGIC = Format::Current::MAGIC

        # Read by 4kB chunks by default
        DEFAULT_BUFFER_READ = 4 * 1024

        # The underlying IO
        attr_reader :io

        # The amount of bytes that should be read into the internal buffer
        #
        # @return [Integer]
        attr_reader :buffer_read

        # Whether the data in the file is stored in little or big endian
        def big_endian?
            @big_endian
        end

        # Create a BlockStream object that acts on a given file
        def self.open(path)
            if block_given?
                File.open(path) do |io|
                    yield(new(io))
                end
            else
                new(File.open(path))
            end
        end

        # Create a {BlockStream} object to sequentially interpret a stream of data
        def initialize(io, buffer_read: DEFAULT_BUFFER_READ)
            @io = io
            @big_endian = nil
            @native_endian = nil
            @payload_size = 0
            @buffer_io = StringIO.new
            @buffer_read = buffer_read
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

        def initialize_copy(old)
            super

            @io = @io.dup
            @buffer_io = StringIO.new(@buffer_io.string.dup)
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

        # Move to the beginning of the stream
        def rewind
            seek(0)
        end

        # Seek to the current raw position in the IO
        #
        # The new position is assumed to be at the start of a block
        def seek(pos)
            io.seek(pos)
            @buffer_io = StringIO.new
            @payload_size = 0
        end

        # @api private
        #
        # Read bytes
        def read(size)
            remaining =
                if (data = @buffer_io.read(size))
                    (size - data.size)
                else
                    size
                end

            return data if remaining == 0

            @buffer_io = StringIO.new(io.read([buffer_read, remaining].max) || '')
            if (buffer_data = @buffer_io.read(remaining))
                (data || '') + buffer_data
            else
                data
            end
        end

        # If the IO is a file, it starts with a prologue to describe the file
        # format
        #
        # Raises MissingPrologue if no prologue is found, or ObsoleteVersion if
        # the file format is not up-to-date (in which case one has to run
        # pocolog --to-new-format).
        def read_prologue_raw # :nodoc:
            Format::Current.read_prologue_raw(io)
        end

        # If the IO is a file, it starts with a prologue to describe the file
        # format
        #
        # Raises MissingPrologue if no prologue is found, or ObsoleteVersion if
        # the file format is not up-to-date (in which case one has to run
        # pocolog --to-new-format).
        def read_prologue # :nodoc:
            big_endian = Format::Current.read_prologue(io)
            @format_version = Format::Current::VERSION
            @big_endian = big_endian
            @native_endian = ((big_endian != 0) ^ Pocolog.big_endian?)
            @payload_size = 0
        end

        BlockHeader = Struct.new :kind, :stream_index, :payload_size, :raw_data do
            def self.parse(raw_header)
                type, index, payload_size = raw_header.unpack('CxvV')
                new(type, index, payload_size, raw_header)
            end
        end

        def self.read_block_header_raw(io, pos = nil)
            io.seek(pos, IO::SEEK_SET) if pos
            io.read(BLOCK_HEADER_SIZE)
        end

        def self.read_block_header(io, pos = nil)
            BlockHeader.parse(read_block_header_raw(io, pos))
        end

        # Read the bytes from the block header at the current position
        #
        # Unlike {#read_next_block_header}, it does not skip remaining payload
        # bytes, and does not prepare the stream to
        def read_block_header_raw
            return unless (header = read(BLOCK_HEADER_SIZE))

            if header.size != BLOCK_HEADER_SIZE
                raise NotEnoughData,
                      "truncated block header (got #{header.size} bytes, "\
                      "expected #{BLOCK_HEADER_SIZE})"
            end

            header
        end

        # Read the header of the next block
        def read_next_block_header
            skip(@payload_size) if @payload_size != 0

            return unless (header = read_block_header_raw)

            block = BlockHeader.parse(header)
            @payload_size = block.payload_size
            block
        end

        # Read the bytes from the block at the current position
        #
        # @return [(BlockHeader, String, String)]
        def read_block_raw
            header_raw = read_block_header_raw
            header = BlockHeader.parse(header_raw)
            payload = read(header.payload_size)
            if !payload || payload.size != header.payload_size
                raise NotEnoughData,
                      "expected to read #{header.payload_size} payload bytes but got "\
                      "#{payload ? payload.size : 'EOF'}"
            end

            [header, header_raw, payload]
        end

        # Information about a stream declaration block
        class StreamBlock
            attr_reader :name
            attr_reader :typename
            attr_reader :registry_xml
            attr_reader :metadata_yaml

            def self.read(raw_data, offset, count)
                if raw_data.size < offset + count
                    raise NotEnoughData,
                          'expected stream block header to be at least of size '\
                          "#{offset + count}, but got only #{raw_data.size}"
                end
                raw_data[offset, count]
            end

            def self.read_string(raw_data, offset)
                size = read(raw_data, offset, 4).unpack('V').first
                [read(raw_data, offset + 4, size), (offset + 4 + size)]
            end

            def self.parse(raw_data)
                name, offset     = read_string(raw_data, 1)
                typename, offset = read_string(raw_data, offset)

                if raw_data.size > offset
                    registry_xml, offset = read_string(raw_data, offset)
                else
                    registry_xml = "<?xml version='1.0'?>\n<typelib></typelib>"
                end

                if raw_data.size > offset
                    metadata_yaml, offset = read_string(raw_data, offset)
                else
                    metadata_yaml = "--- {}\n"
                end
                if offset != raw_data.size
                    raise InvalidBlockFound,
                          "#{raw_data.size - offset} bytes unclaimed in "\
                          'stream declaration block'
                end

                new(name, typename, registry_xml, metadata_yaml)
            end

            def initialize(name, typename, registry_xml, metadata_yaml)
                @name = name
                @typename = typename
                @registry_xml = registry_xml
                @metadata_yaml = metadata_yaml

                @type = nil
                @metadata = nil
            end

            def type
                if @type
                    @type
                else
                    registry = Typelib::Registry.from_xml(registry_xml)
                    @type = registry.build(typename)
                end
            end

            def metadata
                @metadata ||= YAML.safe_load(metadata_yaml)
            end

            # Return the encoded (on-disk) representation of this stream definition
            def encode # rubocop:disable Metrics/AbcSize
                [
                    DATA_STREAM, name.size, name,
                    typename.size, typename,
                    registry_xml.size, registry_xml,
                    metadata_yaml.size, metadata_yaml
                ].pack("CVa#{name.size}Va#{typename.size}"\
                       "Va#{registry_xml.size}Va#{metadata_yaml.size}")
            end
        end

        def self.read_stream_block(io, pos = nil)
            block = read_block(io, pos)
            if block.kind != STREAM_BLOCK
                raise InvalidFile, 'expected stream declaration block'
            end

            StreamBlock.parse(io.read(block.payload_size))
        end

        # Read one stream block
        #
        # The IO is assumed to be positioned at the stream definition's block's payload
        def read_stream_block
            StreamBlock.parse(read_payload)
        end

        # Read the payload of the last block returned by
        # {#read_next_block_header}
        def read_payload(count = @payload_size)
            if count > @payload_size
                raise ArgumentError,
                      "expected read count #{count} greater than remaining "\
                      "payload size #{@payload_size}"
            end

            result = read(count)
            if !result || result.size != count
                raise NotEnoughData,
                      "expected to read #{count} bytes but got "\
                      "#{result ? result.size : 'EOF'}"
            end

            @payload_size -= count
            result
        end

        def skip_payload
            skip(@payload_size)
        end

        # Information about a data block
        class DataBlockHeader
            # Timestamp in realtime for this block, in microseconds
            #
            # @return [Integer]
            # @see rt
            attr_reader :rt_time

            # Timestamp in logical time for this block, in microseconds
            #
            # @return [Integer]
            # @see rt
            attr_reader :lg_time

            # Size in bytes of the marshalled data sample
            #
            # @return [Integer]
            attr_reader :data_size

            # @deprecated whether the sample is compressed
            #
            # Per-sample compression should simply not be used. On anything but
            # gigantic samples it introduces a massive overhead for little gain
            def compressed?
                @compressed
            end

            def self.parse(raw_data)
                if raw_data.size < Format::Current::DATA_BLOCK_HEADER_SIZE
                    raise NotEnoughData,
                          "expected #{Format::Current::DATA_BLOCK_HEADER_SIZE} bytes "\
                          "for a data block header, but got only #{raw_data.size}"
                end

                rt_sec, rt_usec, lg_sec, lg_usec, data_size, compressed =
                    raw_data.unpack("VVVVVC")
                new(rt_sec * 1_000_000 + rt_usec,
                    lg_sec * 1_000_000 + lg_usec,
                    data_size,
                    compressed != 0)
            end

            def initialize(rt_time, lg_time, data_size, compressed)
                @rt_time = rt_time
                @lg_time = lg_time
                @data_size = data_size
                @compressed = compressed
            end

            # Timestamp in realtime
            #
            # @return [Time]
            def rt
                StreamIndex.time_from_internal(rt_time, 0)
            end

            # Timestamp in logical time
            #
            # @return [Time]
            def lg
                StreamIndex.time_from_internal(lg_time, 0)
            end
        end

        # Read the header of one data block
        #
        # The IO is assumed to be positioned at the beginning of the block's
        # payload
        def read_data_block_header
            DataBlockHeader.parse(
                read_payload(Format::Current::DATA_BLOCK_HEADER_SIZE)
            )
        end

        # Read the marshalled version of a data block
        #
        # It splits the block into its header and payload part, and optionally
        # uncompresses the data sample
        def read_data_block(uncompress: true)
            raw_header = read_payload(Format::Current::DATA_BLOCK_HEADER_SIZE)
            raw_data   = read_payload
            compressed = raw_header[-1, 1].unpack('C').first
            if uncompress && (compressed != 0)
                # Payload is compressed
                raw_data = Zlib::Inflate.inflate(raw_data)
            end
            [raw_header, raw_data]
        end

        # Read the data payload of a data block, not parsing the header
        #
        # The IO is assumed to be positioned just after the block header (i.e.
        # after read_next_block_header)
        def read_data_block_payload
            skip(Format::Current::DATA_BLOCK_HEADER_SIZE - 1)
            compressed = read_payload(1).unpack('C').first
            data = read_payload
            if compressed != 0
                # Payload is compressed
                data = Zlib::Inflate.inflate(data)
            end
            data
        end
    end
end
