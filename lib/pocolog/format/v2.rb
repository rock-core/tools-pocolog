# frozen_string_literal: true

module Pocolog
    module Format
        # Implementation of the V2 log format, including the corresponding index file
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
            INDEX_VERSION = 3
            # Size of the index prologue
            INDEX_PROLOGUE_SIZE = INDEX_MAGIC.size + 20
            # Position of the stream count field
            INDEX_STREAM_COUNT_POS = INDEX_PROLOGUE_SIZE
            # Size of the stream count field
            INDEX_STREAM_COUNT_SIZE = 8
            # Size of a stream description in the index
            INDEX_STREAM_DESCRIPTION_SIZE = 8 * 8
            # Size of an entry in the index table
            INDEX_STREAM_ENTRY_SIZE = 8 * 2

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

            # Read the raw bytes from the prologue and return them
            #
            # @return [String]
            # @raise MissingPrologue
            def self.read_prologue_raw(io)
                header = io.read(PROLOGUE_SIZE)
                if !header || (header.size < PROLOGUE_SIZE)
                    raise MissingPrologue, "#{io.path} too small"
                end

                header
            end

            # Read a file's prologue
            #
            # @param [IO] io the file from which to read the prologue
            # @param [Boolean] validate_version if true, the method will raise
            #   if the file version does not match {INDEX_VERSION}
            # @return [(Integer,Boolean)] the file format version and a flag that
            #   tells whether the file's data is encoded as big or little endian
            def self.read_prologue(io, validate_version: true)
                header = read_prologue_raw(io)
                magic = header[0, MAGIC.size]
                if magic != MAGIC
                    raise MissingPrologue,
                          "#{io.path} is not a pocolog log file. "\
                          "Got #{magic} at #{io.tell}, but was expecting #{MAGIC}"
                end

                format_version, big_endian = header[MAGIC.size, 9].unpack("xVV")
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
                          "--to-new-format of pocolog"
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
                io.write(*[VERSION, big_endian ? 1 : 0].pack("xVV"))
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
                    raise InvalidIndex, "index file too small to contain a valid info"
                end

                header = index_io.read(INDEX_MAGIC.size + 4)
                magic = header[0, INDEX_MAGIC.size]
                if magic != INDEX_MAGIC
                    message =
                        if magic
                            "wrong index magic in #{index_io.path}, "\
                            "probably an old index"
                        else
                            "#{index_io.path} is empty"
                        end

                    raise MissingIndexPrologue, message
                end

                index_version = Integer(header[INDEX_MAGIC.size, 4].unpack1("L>"))
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

                index_size, index_mtime = index_io.read(16).unpack("Q>Q>")
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
                index_io.write(data.pack("L>Q>Q>"))
            end

            # Read the information contained in a file index
            #
            # @return [Array<StreamInfo>] the information contained in the index
            #   file
            def self.read_index(index_io, expected_file_size: nil, expected_mtime: nil)
                index_stream_info = read_index_minimal_info(
                    index_io,
                    expected_file_size: expected_file_size,
                    expected_mtime: expected_mtime
                )
                index_stream_info.map { read_stream_info(index_io, _1) }
            end

            # Read {StreamInfo} from the index IO based on the corresponding
            # {IndexStreamInfo}
            #
            # @param [IO] index_io
            # @param [IndexStreamInfo] info
            def self.read_stream_info(index_io, info)
                index_size = info.stream_size * INDEX_STREAM_ENTRY_SIZE
                index_data = index_io.pread(index_size, info.index_pos)
                if index_data.size != index_size
                    raise InvalidIndex, "index file seem truncated"
                end

                index_data = index_data.unpack("Q>*")
                StreamInfo.from_raw_data(
                    info.declaration_pos, info.interval_rt, info.base_time,
                    index_data
                )
            end

            # Read the full stream definition as well as index data for a string without
            # reading any actual data (no index data and no stream data)
            #
            # @return [Array<(BlockStream::StreamBlock,IndexStreamInfo)>]
            def self.read_minimal_info(index_io, file_io, validate: true)
                index_stream_info = read_index_minimal_info(
                    index_io,
                    expected_file_size: (file_io.size if validate)
                )

                index_stream_info.map do |info|
                    file_io.seek(info.declaration_pos)
                    block_stream = BlockStream.new(file_io)
                    block_stream.read_next_block_header
                    stream_block = block_stream.read_stream_block
                    [stream_block, info]
                end
            end

            IndexStreamInfo = Struct.new(
                :declaration_pos,
                :index_pos,
                :base_time,
                :stream_size,
                :rt_min, :rt_max, :lg_min, :lg_max,
                keyword_init: true
            ) do
                def interval_rt
                    if rt_min != rt_max || rt_min
                        [rt_min, rt_max]
                    else
                        []
                    end
                end

                def interval_lg
                    if lg_min != lg_max || lg_min
                        [lg_min, lg_max]
                    else
                        []
                    end
                end
            end

            # Tests whether the index whose path is given is valid for the given
            # log file
            def self.index_file_valid?(index_path, file_path)
                stat = file_path.stat
                begin
                    File.open(index_path) do |index_io|
                        streams_info = read_index_minimal_info(
                            index_io, expected_file_size: stat.size
                        )
                        index_file_validate_size(index_io, streams_info)
                    end
                    true
                rescue Errno::ENOENT
                    false
                end
            rescue InvalidIndex
                false
            end

            # @api private
            #
            # Validate that an index' file size match the value expected from its
            # stream info section
            #
            # @param [IO] index_io
            # @param [Array<IndexStreamInfo>] streams_info
            def self.index_file_validate_size(index_io, streams_info)
                index_end_pos = streams_info.map do |s|
                    s.stream_size * INDEX_STREAM_ENTRY_SIZE + s.index_pos
                end
                expected_file_size = index_end_pos.max
                return if index_io.size == expected_file_size

                raise InvalidIndex,
                      "index file should be of size #{expected_file_size} "\
                      "but is of size #{index_io.size}"
            end

            # Read the stream definitions from an index, but no the index data
            #
            # @return [Array<IndexStreamInfo>]
            def self.read_index_minimal_info(
                index_io, expected_file_size: nil, expected_mtime: nil
            )
                stream_count = read_index_header(
                    index_io,
                    expected_mtime: expected_mtime,
                    expected_file_size: expected_file_size
                )

                read_index_stream_info(index_io, stream_count)
            end

            # Read and optionally validate the header information from an index file
            #
            # @return [Integer] the amount of streams in the index
            # @see write_index_header
            def self.read_index_header(
                index_io, expected_file_size: nil, expected_mtime: nil
            )
                read_index_prologue(index_io,
                                    validate_version: true,
                                    expected_mtime: expected_mtime,
                                    expected_file_size: expected_file_size)

                stream_count_data = index_io.read(8)

                if stream_count_data.size < 8
                    raise InvalidIndex, "index file too small"
                end
                stream_count_data.unpack1("Q>")
            end

            # Read basic stream information from an index file
            #
            # @param [IO] index_io the index IO
            # @return [Array<IndexStreamInfo>] the stream-related information
            #   contained in the index file
            def self.read_index_stream_info(index_io, stream_count)
                stream_count.times.map do
                    read_index_single_stream_info(index_io)
                end
            end

            # Read from IO the index stream info for a single stream
            #
            # @return [IndexStreamInfo]
            def self.read_index_single_stream_info(index_io) # rubocop:disable Metrics/AbcSize
                data = index_io.read(INDEX_STREAM_DESCRIPTION_SIZE)
                if !data || data.size < INDEX_STREAM_DESCRIPTION_SIZE
                    raise InvalidIndex,
                          "not enough data to read stream description in index"
                end

                declaration_pos, index_pos, base_time, stream_size,
                    interval_rt_min, interval_rt_max,
                    interval_lg_min, interval_lg_max =
                    data.unpack("Q>*")

                if stream_size == 0
                    base_time = nil
                    interval_rt = []
                    interval_lg = []
                else
                    interval_rt = [interval_rt_min, interval_rt_max]
                    interval_lg = [interval_lg_min, interval_lg_max]
                end

                IndexStreamInfo.new(
                    declaration_pos: declaration_pos,
                    index_pos: index_pos,
                    base_time: base_time,
                    stream_size: stream_size,
                    rt_min: interval_rt[0],
                    rt_max: interval_rt[1],
                    lg_min: interval_lg[0],
                    lg_max: interval_lg[1]
                )
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
                File.open(index_path, "w") do |index_io|
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
                    raise ArgumentError, "attempting to overwrite the file by its index"
                end

                size = file_io.stat.size
                mtime = file_io.stat.mtime
                write_index_header(index_io, size, mtime, streams.size, version: version)
                write_index_stream_info(index_io, streams)
                write_index_stream_data(index_io, streams)
            end

            # Write index file information before the actual index data
            def self.write_index_header(
                index_io, size, mtime, stream_count, version: INDEX_VERSION
            )
                write_index_prologue(index_io, size, mtime, version: version)
                index_io.write([stream_count].pack("Q>"))
            end

            # Write the stream info part of a file index for all given streams
            def self.write_index_stream_info(index_io, streams)
                index_data_pos = INDEX_STREAM_DESCRIPTION_SIZE * streams.size +
                                 index_io.tell
                streams.each do |stream_info|
                    write_index_single_stream_info(index_io, stream_info, index_data_pos)
                    index_data_pos += stream_info.index.index_map.size * 8
                end
            end

            def self.write_index_single_stream_info(index_io, stream_info, index_data_pos)
                index_stream_info = index_stream_info(stream_info, index_data_pos)
                index_io.write(index_stream_info.to_a.pack("Q>*"))
            end

            # Write the stream data part of a file index for all given streams
            def self.write_index_stream_data(index_io, streams)
                streams.each do |stream_info|
                    index_map = stream_info.index.index_map
                    index_io.write(index_map.pack("Q>*"))
                end
            end

            # @api private
            #
            # Helper method that prepares index contents for a given stream
            def self.index_stream_info(stream_info, index_data_pos)
                interval_rt = stream_info.interval_rt
                interval_lg = stream_info.interval_lg
                base_time   = stream_info.index.base_time
                IndexStreamInfo.new(
                    declaration_pos: stream_info.declaration_blocks.first,
                    index_pos: index_data_pos,
                    base_time: base_time || 0,
                    stream_size: stream_info.size,
                    rt_min: interval_rt[0] || 0,
                    rt_max: interval_rt[1] || 0,
                    lg_min: interval_lg[0] || 0,
                    lg_max: interval_lg[1] || 0
                )
            end
        end
    end
end
