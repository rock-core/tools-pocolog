module Pocolog
    # @api private
    #
    # Information about a stream for indexing purposes
    class StreamInfo
        # Positions of the declaration blocks
        attr_reader :declaration_blocks
        # The position of the first and last samples in the file set, as
        # [[raw_pos, io_index], [raw_pos, io_index]]. It is empty for empty
        # streams.
        attr_reader :interval_io
        # The logical time of the first and last samples of that stream
        # [beginning, end]. It is empty for empty streams.
        attr_reader :interval_lg
        # The real time of the first and last samples of that stream
        # [beginning, end]. It is empty for empty streams.
        attr_reader :interval_rt
        # The number of samples in this stream
        attr_reader :size

        # The index data itself. 
        # This is a instance of StreamIndex
        attr_reader :index

        # True if this stream is empty
        def empty?; size == 0 end

        # Initialize a stream info object from raw information
        def self.from_raw_data(declaration_block, interval_rt, base_time, index_map)
            info = StreamInfo.new
            info.initialize_from_raw_data(declaration_block, interval_rt, base_time, index_map)
            info
        end

        def initialize
            @declaration_blocks = Array.new
            @interval_io = []
            @interval_lg = []
            @interval_rt = []
            @size        = 0
            @index       = StreamIndex.new
        end

        def initialize_copy(copy)
            raise NotImplementedError, "StreamInfo is non-copyable"
        end

        def add_sample(pos, rt, lg)
            if !@interval_io[0]
                @interval_io[0] = @interval_io[1] = pos
                @interval_rt[0] = @interval_rt[1] = Integer(rt)
                @interval_lg[0] = @interval_lg[1] = Integer(lg)
            else
                if pos <= @interval_io[1]
                    raise ArgumentError, "attempting to go back in stream in StreamInfo#add_sample (from #{@interval_io[1]} to #{pos}"
                elsif rt < @interval_rt[1]
                    raise ArgumentError, "attempting to go back in time in StreamInfo#add_sample (from #{@interval_rt[1]} to #{rt}"
                elsif lg < @interval_lg[1]
                    raise ArgumentError, "attempting to go back in time in StreamInfo#add_sample (from #{@interval_lg[1]} to #{lg}"
                end
                @interval_io[1]   = pos
                @interval_rt[1]   = rt
                @interval_lg[1]   = lg
            end
            @size += 1
            index.add_sample(pos, lg)
        end

        # When using IO sequences, use this to append information about the same
        # stream coming from a separate IO
        #
        # @param [Integer] file_pos_offset an offset that should be applied on
        #   all file positions within stream_info. This is used to concatenate
        #   streaminfo objects for streams backed by a {IOSequence}
        def concat(stream_info, file_pos_offset = 0)
            return if stream_info.empty?

            stream_interval_io = stream_info.interval_io.map { |v| v + file_pos_offset }
            if empty?
                interval_io[0] = stream_interval_io[0]
                interval_lg[0] = stream_info.interval_lg[0]
                interval_rt[0] = stream_info.interval_rt[0]
            else
                if @interval_io[1] >= stream_interval_io[0]
                    raise ArgumentError, "the IO range of the given stream starts before the range of self"
                elsif @interval_lg[1] > stream_info.interval_lg[0]
                    raise ArgumentError, "the logical time range of the given stream starts (#{stream_info.interval_lg[0]}) before the range of self (#{@interval_lg[1]})"
                elsif @interval_rt[1] > stream_info.interval_rt[0]
                    raise ArgumentError, "the realtime range of the given stream starts before the range of self"
                end
            end

            interval_io[1] = stream_interval_io[1]
            interval_lg[1] = stream_info.interval_lg[1]
            interval_rt[1] = stream_info.interval_rt[1]

            @size += stream_info.size
            index.concat(stream_info.index, file_pos_offset)
        end

        # Initializes self based on raw information
        #
        # This is used when marshalling/demarshalling index data
        def initialize_from_raw_data(declaration_block, interval_rt, base_time, index_map)
            @declaration_blocks = [declaration_block]
            @index = StreamIndex.from_raw_data(base_time, index_map)
            @interval_rt = interval_rt
            @size = index.size
            if !index.empty?
                @interval_io =
                    [index.file_position_by_sample_number(0),
                     index.file_position_by_sample_number(-1)]
                @interval_lg =
                    [index.internal_time_by_sample_number(0) + index.base_time,
                     index.internal_time_by_sample_number(-1) + index.base_time]
            end
        end
    end
end

