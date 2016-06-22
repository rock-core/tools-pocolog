module Pocolog
    module Format
        module V2
            # The magic code present at the beginning of each pocolog file
            MAGIC = "POCOSIM"
            # Format version ID. Increment this when the file format changes in a
            # non-backward-compatible way
            VERSION = 2
            # The size of the file's prologue
            PROLOGUE_SIZE = MAGIC.size + 9

            # The magic code at the beginning of a pocolog index
            INDEX_MAGIC = "POCOSIM_INDEX"
            # The current index version. Unlike with the format version, a
            # changing index version will only cause rebuilding the index
            #
            # (i.e. this can change without changing the overall format version)
            INDEX_VERSION = 2
            # Size of the index prologue
            INDEX_PROLOGUE_SIZE = INDEX_MAGIC.size + 20

            # The size of the generic block header
            BLOCK_HEADER_SIZE = 8
            # The size of a time in a block header
            TIME_SIZE = 8
            # The size of a data header, excluding the generic block header
            DATA_BLOCK_HEADER_SIZE = TIME_SIZE * 2 + 5

            # Read a file's prologue
            #
            # @param [IO] io the file from which to read the prologue
            # @param [Boolean] validate_version if true, the method will raise
            #   if the file version does not match {INDEX_VERSION}
            # @return [(Integer,Boolean)] the file format version and a flag that
            #   tells whether the file's data is encoded as big or little endian
            def self.read_prologue(io, validate_version: true)
                header = io.read(PROLOGUE_SIZE) || ""
                magic = header[0, MAGIC.size]
                if magic != MAGIC
                    if !magic
                        raise MissingPrologue, "#{io.path} is empty"
                    else
                        raise MissingPrologue, "#{io.path} is not a pocolog log file. Got #{magic} at #{io.tell}, but was expecting #{MAGIC}"
                    end
                end

                format_version, big_endian = header[MAGIC.size, 9].unpack('xVV')
                if validate_version
                    if format_version < VERSION
                        raise ObsoleteVersion, "old format #{format_version}, current format is #{VERSION}. Convert it using the --to-new-format of pocolog"
                    elsif format_version > VERSION
                        raise InvalidFile, "this file is in v#{format_version} which is newer that the one we know #{VERSION}. Update pocolog"
                    end
                end

                return format_version, big_endian
            end

            def self.valid_file?(file)
                File.open(file) do |io|
                    read_prologue(io)
                    true
                end
            rescue InvalidFile
                false
            end

            # Write a v2 file prologue
            def self.write_prologue(io, big_endian = Pocolog.big_endian?)
                io.write(MAGIC)
                io.write(*[VERSION, big_endian ? 1 : 0].pack('xVV'))
            end

            # Read the prologue of an index file
            #
            # @param [Boolean] validate_version if true, the method will raise
            #   if the file version does not match {INDEX_VERSION}
            # @return [Integer] the index file version
            def self.read_index_prologue(index_io, validate_version: true, expected_mtime: nil, expected_file_size: nil)
                if index_io.size < INDEX_PROLOGUE_SIZE
                    raise InvalidIndex, "index file too small to contain a valid index"
                end

                header = index_io.read(INDEX_MAGIC.size + 4)
                magic = header[0, INDEX_MAGIC.size]
                if magic != INDEX_MAGIC
                    if !magic
                        raise MissingIndexPrologue, "#{index_io.path} is empty"
                    else
                        raise MissingIndexPrologue, "#{index_io.path} is missing the index prologue, probably an old index"
                    end
                end
                index_version = Integer(header[INDEX_MAGIC.size, 4].unpack("L>").first)
                if validate_version
                    if index_version < INDEX_VERSION
                        raise ObsoleteIndexVersion, "old format #{format_version}, current format is #{VERSION}. Convert it using the --to-new-format of pocolog"
                    elsif index_version > INDEX_VERSION
                        raise InvalidIndex, "old format #{format_version}, current format is #{VERSION}. Convert it using the --to-new-format of pocolog"
                    end
                end
                file_size, file_mtime = index_io.read(16).unpack("Q>Q>")
                if expected_file_size && expected_file_size != file_size
                    raise InvalidIndex, "file size in index (#{file_size}) and actual file size (#{expected_file_size}) mismatch"
                end
                if expected_mtime && StreamIndex.time_to_internal(expected_mtime, 0) != file_mtime
                    raise InvalidIndex, "file size in index (#{file_size}) and actual file size (#{expected_file_size}) mismatch"
                end
                return index_version, file_size, StreamIndex.time_from_internal(file_mtime, 0)
            end

            # Write a prologue on an index file
            def self.write_index_prologue(index_io, size, mtime)
                index_io.write(INDEX_MAGIC)
                index_io.write([INDEX_VERSION, size, StreamIndex.time_to_internal(mtime, 0)].pack("L>Q>Q>"))
            end

            # Read the information contained in a file index
            def self.read_index(index_io, expected_file_size: nil, expected_mtime: nil)
                read_index_prologue(index_io, validate_version: true,
                                    expected_mtime: expected_mtime,
                                    expected_file_size: expected_file_size)
                if index_io.size < INDEX_PROLOGUE_SIZE + 8
                    raise InvalidIndex, "index file too small"
                end

                stream_count = index_io.read(8).unpack("Q>").first
                if index_io.size < INDEX_PROLOGUE_SIZE + 8 + (24 + 24) * stream_count
                    raise InvalidIndex, "index file too small"
                end

                streams = Array.new
                stream_count.times do
                    # This is (declaration_pos, index_pos, index_size)
                    streams << index_io.read(24).unpack("Q>Q>Q>")
                end

                streams = streams.map do |declaration_pos, index_pos, index_size|
                    index_io.seek(index_pos)
                    index_data = index_io.read(index_size)
                    if index_data.size != index_size
                        raise InvalidIndex, "not enough data in index"
                    end

                    *interval_rt, base_time = index_data[0, 24].unpack("Q>Q>Q>")
                    index_data = index_data[24, index_size - 24].unpack("Q>*").
                        each_slice(3).to_a
                    if index_data.empty?
                        interval_rt = []
                        base_time = nil
                    end
                    StreamInfo.from_raw_data(declaration_pos, interval_rt, base_time, index_data)
                end
            end

            # Write an index file for a given file
            #
            # @param [File] file_io the file that is being indexed. It cannot
            #   be a IOSequence
            # @param [File] index_io the file into which the index should be
            #   written
            # @param [Array<StreamInfo>] streams the stream information that
            #   should be stored
            def self.write_index(index_io, file_io, streams)
                if index_io.path == file_io.path
                    raise ArgumentError, "attempting to overwrite the file by its index"
                end

                write_index_prologue(index_io, file_io.size, file_io.stat.mtime)
                index_io.write([streams.size].pack("Q>"))

                index_list_pos = index_io.tell
                index_data_pos = 24 * streams.size + index_list_pos

                streams.each_with_index do |stream_info, stream_index|
                    interval_rt = stream_info.interval_rt.dup
                    base_time   = stream_info.index.base_time
                    index_data = [interval_rt[0] || 0, interval_rt[1] || 0, base_time || 0].pack("Q>Q>Q>") +
                        stream_info.index.index_map.flatten.pack("Q>*")

                    index_io.seek(index_list_pos)
                    index_io.write([stream_info.declaration_blocks.first, index_data_pos, index_data.size].pack("Q>*"))
                    index_io.seek(index_data_pos)
                    index_io.write(index_data)

                    index_list_pos += 24
                    index_data_pos += index_data.size
                end
            end
        end
    end
end

