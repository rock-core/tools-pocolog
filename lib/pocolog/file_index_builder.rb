module Pocolog
    def self.file_index_builder(block_stream)
        # We build the information expected by StreamInfo.from_raw_data
        # That is (declaration_block, interval_rt, base_time, index_map)
        #
        # The first pass extracts information from the file itself, but
        # avoiding as much tests-in-the-loop as possible. This array
        # therefore only stores the declaration block and index map
        raw_stream_info = Array.new
        block_pos = block_stream.tell
        while block = block_stream.read_next_block_header
            stream_index = block.stream_index

            if block.kind == STREAM_BLOCK
                raw_stream_info[stream_index] = [block_pos, Array.new]
            elsif block.kind == DATA_BLOCK
                data_block = block_stream.read_data_block_header
                index_map  = raw_stream_info[stream_index].last
                index_map << [block_pos, data_block.lg_time, index_map.size]
            end
            block_stream.skip_payload
            block_pos = block_stream.tell
        end

        raw_stream_info.map do |raw_info|
            next if !raw_info

            declaration_block, index_map = raw_info
            interval_rt = Array.new
            base_time = nil
            # Read the realtime of the first and last samples
            if !index_map.empty?
                block_stream.seek(index_map[0][0])
                block_stream.read_next_block_header
                first_block = block_stream.read_data_block_header

                block_stream.seek(index_map[-1][0])
                block_stream.read_next_block_header
                last_block = block_stream.read_data_block_header
                interval_rt = [first_block.rt_time, last_block.rt_time]

                base_time = index_map[0][1]
                index_map.each do |entry|
                    entry[1] -= base_time
                end
            end
            StreamInfo.from_raw_data(declaration_block, interval_rt, base_time, index_map)
        end
    end
end
