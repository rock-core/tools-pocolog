module Pocolog
    # Present a sequence of IOs as one aggregate IO
    #
    # Pocolog can read sequence of log files. This class presents such a
    # sequence as a single IO
    #
    # It is very limited, tailored to be used in pocolog
    class IOSequence
        # The IO sequence itself
        attr_reader :ios
        # The overall stream size in bytes
        attr_reader :size

        def initialize(*ios)
            @size = 0
            @ios = Array.new
            ios.each do |io|
                add_io(io)
            end
        end

        def num_io
            ios.size
        end

        def add_io(io)
            io_size = io.size
            @size += io_size
            ios << [io, io_size]
            if ios.size == 1
                rewind
            end
        end

        def flush
            each_io(&:flush)
        end

        def closed?
            each_io.any?(&:flush)
        end

        def close
            each_io(&:close)
        end

        def each_io
            return enum_for(__method__) if !block_given?
            ios.each do |io, _io_size|
                yield(io)
            end
        end

        def eof?
            (@current_io == ios.last) && @current_io.eof?
        end

        def open
            @io = io.map do |file|
                if file.closed?
                    File.open(file.path)
                else file
                end
            end
        end

        # Seek to the beginning of the file
        def rewind
            return if ios.empty?

            select_io_from_pos(0)
        end

        # The current position in the sequence
        def tell
            @current_io_start + @current_io.tell
        end

        # Seek to the given absolute position in the sequential stream
        #
        # @raise (see select_io_from_pos)
        def seek(pos)
            if pos < @current_io_start || @current_io_end <= pos
                select_io_from_pos(pos)
            end
            @current_io.seek(pos - @current_io_start, IO::SEEK_SET)
        end

        # Read a certain amount of bytes in the underlying IO
        #
        # A read can never cross IO boundaries (i.e. it will only be served by a
        # single file)
        def read(byte_count)
            buffer = @current_io.read(byte_count)
            if !buffer
                select_next_io
                @current_io.read(byte_count)
            else
                buffer
            end
        end

        # @api private
        #
        # Selects the IO that can provide the given position, and sets the
        # relevant internal state accordingly
        #
        # @raise RangeError if the position is outside range
        def select_io_from_pos(pos)
            current_start = 0
            matching_io_index = ios.index do |io, size|
                current_end = current_start + size
                if pos < current_end
                    true
                else
                    current_start = current_end
                    false
                end
            end
            if !matching_io_index
                raise RangeError, "#{pos} is out of range"
            end

            matching_io, matching_io_size = ios[matching_io_index]
            @current_io_index = matching_io_index
            @current_io       = matching_io
            @current_io_start = current_start
            @current_io_end   = current_start + matching_io_size
        end

        # @api private
        #
        # Go to the next IO
        #
        # It does nothing if the current IO is the last
        def select_next_io
            io, size = ios[@current_io_index + 1]
            if io
                @current_io_index += 1
                @current_io = io
                io.rewind
                @current_io_start = @current_io_end
                @current_io_end += size
            end
        end
    end
end

