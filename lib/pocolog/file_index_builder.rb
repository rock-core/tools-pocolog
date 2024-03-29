# frozen_string_literal: true

module Pocolog
    # Raw stream information gathered while passing through a log file, to build
    # an index
    #
    # @!method stream_block_pos
    #   @return [Integer] the position of the stream definition in the IO
    #
    # @!method index_map
    #   @return [Array<(Integer,Integer,Integer)>] at list of
    #   (block_pos,lg_time,sample_index) tuples. The logical time is in
    #   absolute microseconds.
    IndexBuilderStreamInfo = Struct.new :stream_block_pos, :index_map

    # Build the index of a block stream
    #
    # @param [BlockStream] block_stream the block stream that represents the log
    #   file
    # @return [Array<StreamInfo,nil>]
    def self.file_index_builder(block_stream, skip_payload: true)
        # We build the information expected by StreamInfo.from_raw_data
        # That is (declaration_block, interval_rt, base_time, index_map)
        #
        # The first pass extracts information from the file itself, but
        # avoiding as much tests-in-the-loop as possible. This array
        # therefore only stores the declaration block and index map
        raw_stream_info = []
        block_pos = block_stream.tell
        interval_rt = []
        while (block = block_stream.read_next_block_header)
            stream_index = block.stream_index
            interval_rt[stream_index] ||= []

            if block.kind == STREAM_BLOCK
                raw_stream_info[stream_index] =
                    IndexBuilderStreamInfo.new(block_pos, [])
            elsif block.kind == DATA_BLOCK
                data_block = block_stream.read_data_block_header
                interval_rt[stream_index][0] ||= data_block.rt_time
                interval_rt[stream_index][1] = data_block.rt_time
                index_map = raw_stream_info[stream_index].index_map
                index_map << block_pos << data_block.lg_time
            end

            if skip_payload
                block_stream.skip_payload
            else
                block_stream.read_payload
            end
            block_pos = block_stream.tell
        end
        create_index_from_raw_info(
            block_stream, raw_stream_info, interval_rt: interval_rt
        )
    end

    # Create an list of {StreamInfo} object based on basic information about the
    # file
    #
    # @param [Array<IndexBuilderStreamInfo>] raw_info minimal stream information
    #   needed to build the index
    # @return [Array<StreamInfo,nil>]
    def self.create_index_from_raw_info(block_stream, raw_info, interval_rt: [])
        raw_info.each_with_index.map do |stream_info, stream_i|
            next unless stream_info

            index_map = stream_info.index_map
            base_time = nil
            stream_interval_rt = interval_rt[stream_i]

            unless index_map.empty?
                # Read the realtime of the first and last samples if they are
                # not provided
                stream_interval_rt ||= create_index_read_interval_rt(block_stream, index_map)

                base_time = index_map[1]
                index_map = StreamIndex.map_entries_internal(index_map) do |pos, time|
                    [pos, time - base_time]
                end
            end

            StreamInfo.from_raw_data(
                stream_info.stream_block_pos,
                stream_interval_rt || [], base_time, index_map
            )
        end
    end

    def self.create_index_read_interval_rt(block_stream, index_map)
        block_stream.seek(index_map.first)
        block_stream.read_next_block_header
        first_block = block_stream.read_data_block_header

        block_stream.seek(index_map[-2])
        block_stream.read_next_block_header
        last_block = block_stream.read_data_block_header
        [first_block.rt_time, last_block.rt_time]
    end
end
