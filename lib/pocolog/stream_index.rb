module Pocolog
    # @api private
    #
    # Index containing the information for a given stream
    #
    # The stream can possibly span multiple files if it is backed by a
    # {IOSequence} object. This is transparent to the index as {IOSequence}
    # provides an interface where positions span the IOs
    class StreamIndex
        attr_reader :base_time

        # The index information
        #
        # This is an array of tuples.
        #
        # REALLY IMPORTANT: the tuples MUST be of size 3 or lower. On CRuby,
        # this ensures that the elements are stored within the array object
        # itself instead of allocating extra memory on the heap.
        attr_reader :index_map

        def initialize
            @base_time = nil
            @index_map = Array.new
	end

        def initialize_copy(copy)
            raise NotImplementedError, "StreamInfo is non-copyable"
        end

        # Change this stream's base time
        #
        # @param [Integer,Time] value the new base time, either in StreamIndex's
        #   internal representation, or as a Time object
        def base_time=(value)
            if value.respond_to?(:to_time)
                value = StreamIndex.time_to_internal(value, 0)
            end

            @base_time ||= value
            offset = @base_time - value
            return if offset == 0
            @index_map = index_map.map do |file_pos, time, sample_index|
                [file_pos, time + offset, sample_index]
            end
            @base_time = value
        end

        # Number of samples in this index
        def size
            @index_map.size
        end

        # Concatenates followup information for the same stream
        #
        # @param [StreamIndex] stream_index the index to concatenate
        # @param [Integer] file_pos_offset offset to apply to the added stream's
        #   file position. This is used when building the index of a stream
        #   backed by a {IOSequence}.
        def concat(stream_index, file_pos_offset = 0)
            @base_time ||= stream_index.base_time

            time_offset         = stream_index.base_time - base_time
            sample_index_offset = size
            stream_index.index_map.each do |file_pos, time, sample_index|
                index_map << [file_pos + file_pos_offset,
                              time + time_offset,
                              sample_index + sample_index_offset]
            end
        end
	
        # Append a new sample to the index
	def add_sample(pos, time)
            @base_time ||= StreamIndex.time_to_internal(time, 0)
            @index_map << [pos, StreamIndex.time_to_internal(time, @base_time), @index_map.size]
	end

        # Create a Time object from the index' own internal Time representation
        def self.time_from_internal(time, base_time)
            time = time + base_time
            Time.at(time / 1_000_000, time % 1_000_000)
        end

        # Converts a Time object into the index' internal representation
        def self.time_to_internal(time, base_time)
            internal = time.tv_sec * 1_000_000 + time.tv_usec
            internal - base_time
        end

        # Returns the sample number of the first sample whose time is not before
        # the given time
        #
        # @param [Time]
	def sample_number_by_time(sample_time)
            sample_time = StreamIndex.time_to_internal(sample_time, base_time)
            sample_number_by_internal_time(sample_time)
        end

        # Returns the sample number of the first sample whose time is not before
        # the given time
        #
        # @param [Integer]
        def sample_number_by_internal_time(sample_time)
            _pos_, _time, idx = @index_map.bsearch { |_, t, _| t >= sample_time }
            idx || size
	end
	
	# Returns the IO position of a sample
        #
        # @param [Integer] sample_number the sample index in the stream
        # @return [Integer] the sample's position in the backing IO
        # @raise IndexError if the sample number is out of bounds
	def file_position_by_sample_number(sample_number)
            @index_map.fetch(sample_number)[0]
	end

        # Returns the time of a sample
        #
        # @param [Integer] sample_number the sample index in the stream
        # @return [Integer] the sample's time in the index' internal encoding
        # @raise IndexError if the sample number is out of bounds
        def internal_time_by_sample_number(sample_number)
            @index_map.fetch(sample_number)[1]
        end

        # Returns the time of a sample
        #
        # @param [Integer] sample_number the sample index in the stream
        # @return [Time] the sample's time in the index' internal encoding
        # @raise (see internal_time_by_sample_number)
        def time_by_sample_number(sample_number)
            StreamIndex.time_from_internal(internal_time_by_sample_number(sample_number), base_time)
	end
    end
end
