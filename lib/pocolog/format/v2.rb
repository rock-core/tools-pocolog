# frozen_string_literal: true

module Pocolog
    module Format
        module V2
            # The magic code present at the beginning of each pocolog file
            MAGIC = 'POCOSIM'
            # Format version ID. Increment this when the file format changes in a
            # non-backward-compatible way
            VERSION = 2
            # The size of the file's prologue
            PROLOGUE_SIZE = MAGIC.size + 9

            # The magic code at the beginning of a pocolog index
            INDEX_MAGIC = 'POCOSIM_INDEX'
            # The current index version. Unlike with the format version, a
            # changing index version will only cause rebuilding the index
            #
            # (i.e. this can change without changing the overall format version)
            INDEX_VERSION = 3
            # Size of the index prologue
            INDEX_PROLOGUE_SIZE = INDEX_MAGIC.size + 20
            # Size of a stream description in the index
            INDEX_STREAM_DESCRIPTION_SIZE = 8 * 8
            # Size of an entry in the index table
            INDEX_STREAM_ENTRY_SIZE = 8 * 3

            # The size of the generic block header
            BLOCK_HEADER_SIZE = 8
            # The size of a time in a block header
            TIME_SIZE = 8
            # The size of a data header, excluding the generic block header
            DATA_BLOCK_HEADER_SIZE = TIME_SIZE * 2 + 5

            # The size of a stream block declaration header
            #
            # Stream declarations contain variable-length strings, we can only
            # have a min
            STREAM_BLOCK_DECLARATION_HEADER_SIZE_MIN = 9

            # Read a file's prologue
            #
            # @param [IO] io the file from which to read the prologue
            # @param [Boolean] validate_version if true, the method will raise
            #   if the file version does not match {INDEX_VERSION}
            # @return [(Integer,Boolean)] the file format version and a flag that
            #   tells whether the file's data is encoded as big or little endian
            def self.read_prologue(io, validate_version: true)
                header = io.read(PROLOGUE_SIZE) || ''
                if !header || (header.size < PROLOGUE_SIZE)
                    raise MissingPrologue, "#{io.path} too small"
                end

                magic = header[0, MAGIC.size]
                if magic != MAGIC
                    raise MissingPrologue,
                          "#{io.path} is not a pocolog log file. "\
                          "Got #{magic} at #{io.tell}, but was expecting #{MAGIC}"
                end

                format_version, big_endian = header[MAGIC.size, 9].unpack('xVV')
                validate_version(format_version) if validate_version
                [format_version, big_endian]
            end

            # Verify that the given version is compatible with this format version
            #
            # @raise ObsoleteVersion if the version is older than {VERSION} and
            #        cannot be loaded by this code
            # @raise InvalidFile if the version is newer than {VERSION}
            def self.validate_version(version)
                if version < VERSION
                    raise ObsoleteVersion,
                          "old format #{version}, current format "\
                          "is #{VERSION}. Convert it using the "\
                          '--to-new-format of pocolog'
                elsif version > VERSION
                    raise InvalidFile,
                          "this file is in v#{version} which is "\
                          "newer that the one we know #{VERSION}. Update pocolog"
                end
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
            def self.read_index_prologue(
                index_io, validate_version: true,
                expected_mtime: nil, expected_file_size: nil
            )
                if index_io.size < INDEX_PROLOGUE_SIZE
                    raise InvalidIndex, 'index file too small to contain a valid index'
                end

                header = index_io.read(INDEX_MAGIC.size + 4)
                magic = header[0, INDEX_MAGIC.size]
                if magic != INDEX_MAGIC
                    message =
                        if magic
                            "wrong index magic in #{index_io.path}, "\
                            'probably an old index'
                        else
                            "#{index_io.path} is empty"
                        end

                    raise MissingIndexPrologue, message
                end

                index_version = Integer(header[INDEX_MAGIC.size, 4].unpack('L>').first)
                if validate_version
                    if index_version < INDEX_VERSION
                        raise ObsoleteIndexVersion,
                              "old format #{index_version}, "\
                              "current format is #{INDEX_VERSION}"
                    elsif index_version > INDEX_VERSION
                        raise InvalidIndex,
                              "old format #{index_version}, "\
                              "current format is #{INDEX_VERSION}"
                    end
                end

                index_size, index_mtime = index_io.read(16).unpack('Q>Q>')
                if expected_file_size && expected_file_size != index_size
                    raise InvalidIndex,
                          "file size in index (#{index_size}) and actual file "\
                          "size (#{expected_file_size}) mismatch"
                end
                if expected_mtime
                    expected_mtime_i = StreamIndex.time_to_internal(expected_mtime, 0)
                    if expected_mtime_i != index_mtime
                        raise InvalidIndex,
                              "mtime in index (#{index_mtime}) and actual mtime "\
                              "(#{expected_mtime_i}) mismatch"
                    end
                end
                [index_version, index_size,
                 StreamIndex.time_from_internal(index_mtime, 0)]
            end

            # Write a prologue on an index file
            def self.write_index_prologue(index_io, size, mtime, version: INDEX_VERSION)
                index_io.write(INDEX_MAGIC)
                data = [version, size, StreamIndex.time_to_internal(mtime, 0)]
                index_io.write(data.pack('L>Q>Q>'))
            end

            # Read the information contained in a file index
            #
            # @return [Array<StreamInfo>] the information contained in the index
            #   file
            def self.read_index(index_io, expected_file_size: nil, expected_mtime: nil)
                minimal_stream_info = read_index_stream_info(
                    index_io,
                    expected_file_size: expected_file_size,
                    expected_mtime: expected_mtime
                )

                minimal_stream_info.map do |info|
                    index_size = info.stream_size * INDEX_STREAM_ENTRY_SIZE
                    index_io.seek(info.index_pos)
                    index_data = index_io.read(index_size)
                    if index_data.size != index_size
                        raise InvalidIndex, 'not enough or too much data in index'
                    end

                    index_data = index_data.unpack('Q>*')
                                           .each_slice(3).to_a
                    StreamInfo.from_raw_data(
                        info.declaration_pos, info.interval_rt, info.base_time,
                        index_data
                    )
                end
            end

            # @!method declaration_pos
            #   @return [Integer] the position in the pocolog file of the stream
            #     declaration block
            #
            # @!method index_pos
            #   @return [Integer] the position in the index file of the stream
            #     index data
            #
            # @!method stream_size
            #   @return [Integer] the number of samples in the stream
            IndexStreamInfo = Struct.new(
                :declaration_pos, :index_pos, :base_time, :stream_size,
                :interval_rt, :interval_lg
            )

            # Tests whether the index whose path is given is valid for the given
            # log file
            def self.index_file_valid?(index_path, file_path)
                stat = file_path.stat
                begin
                    File.open(index_path) do |index_io|
                        read_index_stream_info(index_io, expected_file_size: stat.size)
                    end
                    true
                rescue Errno::ENOENT
                    false
                end
            rescue InvalidIndex
                false
            end

            # Read basic stream information from an index file
            #
            # @param [IO] index_io the index IO
            # @return [Array<IndexStreamInfo>] the information contained in the index
            #   file
            def self.read_index_stream_info(index_io,
                                            expected_file_size: nil,
                                            expected_mtime: nil)
                read_index_prologue(index_io,
                                    validate_version: true,
                                    expected_mtime: expected_mtime,
                                    expected_file_size: expected_file_size)

                index_size = index_io.size
                if index_size < INDEX_PROLOGUE_SIZE + 8
                    raise InvalidIndex, 'index file too small'
                end

                stream_count = index_io.read(8).unpack('Q>').first
                minimum_index_size = INDEX_PROLOGUE_SIZE + 8 +
                                     INDEX_STREAM_DESCRIPTION_SIZE * stream_count
                if index_size < minimum_index_size
                    raise InvalidIndex, 'index file too small'
                end

                expected_file_size = []

                streams = []
                stream_count.times do
                    values = index_io.read(INDEX_STREAM_DESCRIPTION_SIZE).unpack('Q>*')
                    # This is (declaration_pos, index_pos, stream_size)
                    declaration_pos, index_pos, base_time, stream_size,
                        interval_rt_min, interval_rt_max,
                        interval_lg_min, interval_lg_max = *values

                    index_size = stream_size * INDEX_STREAM_ENTRY_SIZE
                    expected_file_size << index_size + index_pos

                    if stream_size == 0
                        base_time = nil
                        interval_rt = []
                        interval_lg = []
                    else
                        interval_rt = [interval_rt_min, interval_rt_max]
                        interval_lg = [interval_lg_min, interval_lg_max]
                    end

                    streams << IndexStreamInfo.new(
                        declaration_pos, index_pos, base_time,
                        stream_size, interval_rt, interval_lg
                    )
                end
                expected_file_size = expected_file_size.max
                if index_io.size != expected_file_size
                    raise InvalidIndex,
                          "index file should be of size #{expected_file_size} "\
                          "but is of size #{index_io.size}"
                end

                streams
            end

            # Read the stream information, but not the actual block index, from
            # an index file
            #
            # @return [Array<(BlockStream::StreamBlock,IndexStreamInfo)>]
            def self.read_minimal_stream_info(index_io, file_io)
                index_stream_info = read_index_stream_info(
                    index_io,
                    expected_file_size: file_io.size
                )

                index_stream_info.map do |info|
                    file_io.seek(info.declaration_pos)
                    block_stream = BlockStream.new(file_io)
                    block_stream.read_next_block_header
                    stream_block = block_stream.read_stream_block
                    [stream_block, info]
                end
            end

            # Rebuild a pocolog file's index and saves it to file
            #
            # @param [File] io the pocolog file IO
            # @param [String] index_path the path into which the index should be
            #   saved
            # @return (see Pocolog.file_index_builder)
            def self.rebuild_index_file(io, index_path)
                block_stream = BlockStream.new(io)
                block_stream.read_prologue
                stream_info = Pocolog.file_index_builder(block_stream)
                FileUtils.mkdir_p(File.dirname(index_path))
                File.open(index_path, 'w') do |index_io|
                    write_index(index_io, io, stream_info)
                end
                stream_info
            end

            # Write an index file for a given file
            #
            # @param [File] file_io the file that is being indexed. It cannot
            #   be a IOSequence
            # @param [File] index_io the file into which the index should be
            #   written
            # @param [Array<StreamInfo>] streams the stream information that
            #   should be stored
            def self.write_index(index_io, file_io, streams, version: INDEX_VERSION)
                if index_io.path == file_io.path
                    raise ArgumentError, 'attempting to overwrite the file by its index'
                end

                write_index_prologue(index_io, file_io.stat.size, file_io.stat.mtime,
                                     version: version)
                index_io.write([streams.size].pack('Q>'))

                index_list_pos = index_io.tell
                index_data_pos = INDEX_STREAM_DESCRIPTION_SIZE * streams.size +
                                 index_list_pos

                streams.each do |stream_info|
                    index_stream_info, index_data =
                        index_contents_from_stream(stream_info, index_data_pos)

                    index_io.seek(index_list_pos)
                    index_io.write(index_stream_info.pack('Q>*'))
                    index_io.seek(index_data_pos)
                    index_io.write(index_data.pack('Q>*'))

                    index_list_pos += INDEX_STREAM_DESCRIPTION_SIZE
                    index_data_pos += index_data.size * 8
                end
            end

            # @api private
            #
            # Helper method that prepares index contents for a given stream
            def self.index_contents_from_stream(stream_info, index_data_pos)
                interval_rt = stream_info.interval_rt.dup
                interval_lg = stream_info.interval_lg.dup
                base_time   = stream_info.index.base_time
                index_stream_info = [
                    stream_info.declaration_blocks.first,
                    index_data_pos,
                    base_time || 0,
                    stream_info.size,
                    interval_rt[0] || 0, interval_rt[1] || 0,
                    interval_lg[0] || 0, interval_lg[1] || 0
                ]

                [index_stream_info, stream_info.index.index_map.flatten]
            end
        end
    end
end
