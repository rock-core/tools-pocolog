# frozen_string_literal: true

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

        # Initialize a stream index object from the raw information
        def self.from_raw_data(base_time, index_map)
            index = StreamIndex.new(base_time: base_time)
            index.index_map.concat(index_map)
            index
        end

        def initialize(base_time: nil)
            @base_time = base_time
            @index_map = []
        end

        def initialize_copy(_copy)
            raise NotImplementedError, 'StreamInfo is non-copyable'
        end

        # Change this stream's base time
        #
        # @param [Integer,Time] value the new base time, either in StreamIndex's
        #   internal representation, or as a Time object
        def base_time=(value)
            value = StreamIndex.time_to_internal(value, 0) if value.respond_to?(:to_time)

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

        # True if there are no samples indexed in self
        def empty?
            @index_map.empty?
        end

        # Iterate over the index
        #
        # @yieldparam [Integer] file_pos the position in the file
        # @yieldparam [Integer] time the time since {#base_time}
        def raw_each
            @index_map.each do |file_pos, time, _|
                yield(file_pos, time)
            end
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
            add_raw_sample(pos, time)
        end

        # Append a new sample to the index
        def add_raw_sample(pos, time)
            @base_time ||= time
            @index_map << [pos, time - @base_time, @index_map.size]
        end

        # Return a new index without any sample before the given time
        #
        # @return [StreamIndex]
        def remove_before(time)
            index = sample_number_by_time(time)
            index_map = (index...size).map do |i|
                e = @index_map[i].dup
                e[2] = i - index
                e
            end
            self.class.from_raw_data(@base_time, index_map)
        end

        # Return a new index without any sample after the given time
        #
        # @return [StreamIndex]
        def remove_after(time)
            index = sample_number_by_time(time)
            self.class.from_raw_data(@base_time, @index_map[0...index])
        end

        # Return the index with only every N-th samples
        def resample_by_index(period)
            self.class.from_raw_data(@base_time, @index_map) if period == 1

            new_map = Array.new((size + period - 1) / period)
            new_map.size.times do |i|
                entry = @index_map[i * period].dup
                entry[2] = i
                new_map[i] = entry
            end
            self.class.from_raw_data(@base_time, new_map)
        end

        # Return the index with only every N-th samples
        def resample_by_time(period, start_time: nil)
            period_us = period * 1_000_000
            next_time =
                if start_time
                    self.class.time_to_internal(start_time, @base_time)
                else
                    internal_time_by_sample_number(0)
                end

            new_map = []
            @index_map.each do |entry|
                entry_t = entry[1]
                if entry_t >= next_time
                    new_map << [entry[0], entry[1], new_map.size]
                    next_time += period_us until entry_t < next_time
                end
            end
            self.class.from_raw_data(@base_time, new_map)
        end

        # Return the time of the first sample
        #
        # @return [Time,nil]
        def start_time
            time_by_sample_number(0) unless empty?
        end

        # Return the time of the last sample
        #
        # @return [Time,nil]
        def end_time
            time_by_sample_number(-1) unless empty?
        end

        # Create a Time object from the index' own internal Time representation
        def self.time_from_internal(time, base_time)
            time += base_time
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
            StreamIndex.time_from_internal(
                internal_time_by_sample_number(sample_number),
                base_time
            )
        end
    end
end
