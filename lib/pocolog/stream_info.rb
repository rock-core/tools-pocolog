module Pocolog
    # Basic information about a stream, as saved in the index files
    class StreamInfo
        STREAM_INFO_VERSION = "1.3"
        
        #the version of the StreamInfo class. This is only used to detect
        #if an old index file is on the disk compared to the code 
        attr_accessor :version
        # Position of the declaration block as [raw_pos, io_index]. This
        # information can directly be given to Logfiles#seek
        attr_accessor :declaration_block
        # The position of the first and last samples in the file set, as
        # [[raw_pos, io_index], [raw_pos, io_index]]. It is empty for empty
        # streams.
        attr_accessor :interval_io
        # The logical time of the first and last samples of that stream
        # [beginning, end]. It is empty for empty streams.
        attr_accessor :interval_lg
        # The real time of the first and last samples of that stream
        # [beginning, end]. It is empty for empty streams.
        attr_accessor :interval_rt
        # The number of samples in this stream
        attr_accessor :size

        # The index data itself. 
        # This is a instance of StreamIndex
        attr_accessor :index

        # True if this stream is empty
        def empty?; size == 0 end

        def initialize
            @version = STREAM_INFO_VERSION
            @interval_io = []
            @interval_lg = []
            @interval_rt = []
            @size        = 0
            @index       = StreamIndex.new
        end

        def append_sample(io_index, pos, rt, lg)
            if !@interval_io[0]
                @interval_io[0] = @interval_io[1] = [io_index, pos]
                @interval_rt[0] = @interval_rt[1] = rt
                @interval_lg[0] = @interval_lg[1] = lg
            else
                if rt < @interval_rt[1]
                    raise ArgumentError, "attempting to go back in time in StreamInfo#append_sample (from #{@interval_rt[1]} to #{rt}"
                elsif lg < @interval_lg[1]
                    raise ArgumentError, "attempting to go back in time in StreamInfo#append_sample (from #{@interval_lg[1]} to #{lg}"
                end
                @interval_io[1]   = [io_index, pos]
                @interval_rt[1]   = rt
                @interval_lg[1]   = lg
            end
            @size += 1
            index.add_sample_to_index(io_index, pos, lg)
        end
    end
end

