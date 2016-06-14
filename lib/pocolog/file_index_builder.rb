module Pocolog
    # Builds an index for a log file
    class FileIndexBuilder
        attr_reader :files
        attr_reader :stream_info

        def initialize
            @files   = Array.new
            @stream_info = Array.new
        end

        # Process the given block stream
        def add(block_stream)
            register_file(block_stream.io)
            add_blocks(block_stream)
        end

        # Add an IO into the set of files backing this index
        def register_file(io)
            files << io
        end

        # Returns the file information as stored in the index file
        def file_info
            files.map do |io|
                io.flush
                [io.size, io.stat.mtime]
            end
        end

        # Add the blocks enumerated by the given block stream to this index
        def add_blocks(block_stream)
            io_index = file_info.size - 1

            seen_streams = Array.new
            block_pos = block_stream.tell
            while block = block_stream.next
                stream_index = block.stream_index

                if block.kind == STREAM_BLOCK
                    add_stream(io_index, block_pos, stream_index)
                    seen_streams[stream_index] = true
                elsif block.kind == DATA_BLOCK
                    data_block = block_stream.read_data_block_header
                    add_block(io_index, block_pos, stream_index, data_block.rt_time, data_block.lg_time)
                end
                block_stream.skip_payload
                block_pos = block_stream.tell
            end
        end

        # Register a new stream in the index
        def add_stream(io_index, block_pos, stream_index)
            info = (stream_info[stream_index] = StreamInfo.new)
            info.declaration_block = [io_index, block_pos]
        end

        # Register a new block in the index
        def add_block(io_index, block_pos, stream_index, rt_time, lg_time)
            info = stream_info.fetch(stream_index)
            info.append_sample(io_index, block_pos, rt_time, lg_time)
        end

        # Save the index in the given filename
        def save(filename)
            File.open(filename, 'w') do |io|
                Marshal.dump([file_info, stream_info], io)
            end
        rescue
            FileUtils.rm_f filename
            raise
        end
    end
end
