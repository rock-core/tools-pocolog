# frozen_string_literal: true

require 'pocolog/cli/null_reporter'

module Pocolog
    # Attempt to repair a broken file
    #
    # @return [Boolean] true if the file was valid, false otherwise
    def self.repair_file(
        path, keep_invalid_files: true, backup: true, reporter: CLI::NullReporter.new
    )
        size = File.stat(path).size
        reporter.reset_progressbar('[:bar]', total: size)
        block_stream = BlockStream.open(path)
        broken_path = "#{path}.broken" if backup

        begin
            block_stream.read_prologue
        rescue InvalidFile
            reporter.error "#{path}: missing or invalid prologue, nothing to salvage"
            return false if keep_invalid_files

            if broken_path
                FileUtils.mv path, broken_path
                reporter.error "moved #{path} to #{broken_path}"
            else
                FileUtils.rm_f path
                reporter.error "deleted #{path}"
            end
            return false
        end

        stream_info = {}
        stream_sample = {}
        current_pos = block_stream.tell

        begin
            while (block = block_stream.read_next_block_header)
                if block.kind == STREAM_BLOCK
                    stream = block_stream.read_stream_block
                    stream_info[block.stream_index] = stream
                    stream_sample[block.stream_index] = stream.type.new
                else
                    _, marshalled_data = block_stream.read_data_block
                    stream_sample[block.stream_index]
                        .from_buffer_direct(marshalled_data)
                end
                current_pos = block_stream.tell
                reporter.current = current_pos
            end
            reporter.finish
            true
        rescue InvalidFile
            reporter.finish
            remaining = File.stat(path).size - current_pos
            reporter.error "#{path}: broken at position #{current_pos} "\
                           "(#{remaining} bytes thrown away)"
            reporter.error "  copying the current file as #{path}.broken"
            FileUtils.cp path, "#{path}.broken"
            reporter.error '  truncating the existing file'
            File.open(path, 'a') { |io| io.truncate(current_pos) }
            false
        end
    end

    # Find the byte position just after the last valid block in the stream
    #
    # It starts the search at the stream's current position
    def self.find_valid_range(block_stream)
        stream_sample = {}
        current_pos = block_stream.tell

        while (block = block_stream.read_next_block_header)
            if block.kind == STREAM_BLOCK
                stream = block_stream.read_stream_block
                stream_sample[block.stream_index] = stream.type.new
            elsif block.kind == DATA_BLOCK
                _, marshalled_data = block_stream.read_data_block
                stream_sample[block.stream_index]
                    .from_buffer_direct(marshalled_data)
                current_pos = block_stream.tell
            end
        end

        current_pos
    rescue InvalidFile
        current_pos
    end
end
