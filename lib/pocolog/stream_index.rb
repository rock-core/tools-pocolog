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
        # Time (in microseconds) used to reference all the other times in the index
        #
        # @return [Integer]
        attr_reader :base_time

        # The index information
        #
        # This is an flat array of file_pos, time for each sample
        attr_reader :index_map

        # Initialize a stream index object from the raw information
        def self.from_raw_data(base_time, index_map)
            raise if index_map.first.kind_of?(Array)

            StreamIndex.new(base_time: base_time, index_map: index_map)
        end

        def initialize(base_time: nil, index_map: [])
            raise if index_map.first.kind_of?(Array)

            @base_time = base_time
            @index_map = index_map
        end

        def initialize_copy(_copy)
            raise NotImplementedError, "StreamInfo is non-copyable"
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

            @index_map = map_entries_internal { |pos, t| [pos, t + offset] }
            @base_time = value
        end

        # Create a new index object with its entries changed
        #
        # @yieldparam [Integer] pos the file position for this sample
        # @yieldparam [Integer] time the internal sample time, as an offset in
        #   microseconds from {#base_time}
        def map_entries(&block)
            new_map = map_entries_internal(&block)
            self.class.from_raw_data(@base_time, new_map)
        end

        # @api private
        #
        # Non-destructively change the internal @index_map object
        def map_entries_internal(&block)
            self.class.map_entries_internal(@index_map, &block)
        end

        # @api private
        #
        # Map entries of an index index map data array
        #
        # @yieldparam [Integer] pos file position
        # @yieldparam [Integer] time internal time
        # @yieldreturn [(Integer, Integer)] new position and time
        # @return [Array] updated index map
        def self.map_entries_internal(index_map)
            new_map = Array.new(index_map.size)
            (0...index_map.size).step(2) do |i|
                new_map[i, 2] = yield(index_map[i, 2])
            end
            new_map
        end

        # Number of samples in this index
        def sample_count
            @index_map.size / 2
        end

        # True if there are no samples indexed in self
        def empty?
            @index_map.empty?
        end

        # Iterate over the index
        #
        # @yieldparam [Integer] file_pos the position in the file
        # @yieldparam [Integer] time the time since {#base_time}
        def raw_each(&block)
            @index_map.each_slice(2, &block)
        end

        # Iterate over the sample times
        #
        # @yieldparam [Integer] time the time since {#base_time} in microseconds
        def raw_each_time
            return enum_for(__method__) unless block_given?

            (1...@index_map.size).step(2) do |i|
                yield(@index_map[i])
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
            time_offset = stream_index.base_time - base_time

            new_index_map = stream_index.map_entries_internal do |pos, time|
                [pos + file_pos_offset, time + time_offset]
            end
            @index_map.concat(new_index_map)
        end

        # Append a new sample to the index
        #
        # @param [Integer] internal_time time as an offset in microseconds from the
        #   base time
        def add_sample(pos, internal_time)
            add_raw_sample(pos, internal_time)
        end

        # Append a new sample to the index
        #
        # @param [Integer] internal_time time as an offset in microseconds from the
        #   base time
        def add_raw_sample(pos, internal_time)
            @base_time ||= internal_time
            @index_map << pos << (internal_time - @base_time)
        end

        # Return a new index without any sample before the given time
        #
        # @param [Time] time
        # @return [StreamIndex]
        def remove_before(time)
            index = sample_number_by_time(time)
            new_map = @index_map[(index * 2)...@index_map.size]
            self.class.from_raw_data(@base_time, new_map)
        end

        # Return a new index without any sample at or after the given time
        #
        # @param [Time] time
        # @return [StreamIndex]
        def remove_after(time)
            index = sample_number_by_time(time)
            new_map = @index_map[0...(index * 2)]
            self.class.from_raw_data(@base_time, new_map)
        end

        # Return the index with only every N-th samples
        def resample_by_index(period)
            return self.class.from_raw_data(@base_time, @index_map) if period == 1

            new_map = Array.new((sample_count + period - 1) / period * 2)
            (0...new_map.size).step(2) do |i|
                j = i * period
                new_map[i] = @index_map[j]
                new_map[i + 1] = @index_map[j + 1]
            end
            self.class.from_raw_data(@base_time, new_map)
        end

        # Return the index with only every N-th samples
        def resample_by_time(period, start_time: nil)
            period_us = Integer(period * 1_000_000)
            next_time =
                if start_time
                    self.class.time_to_internal(start_time, @base_time)
                else
                    internal_time_by_sample_number(0)
                end

            new_map = []
            raw_each do |pos, time|
                if time >= next_time
                    new_map << pos << time
                    next_time += ((time - next_time) / period_us + 1) * period_us
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
            idx = (0...sample_count).bsearch do |i|
                @index_map[i * 2 + 1] >= sample_time
            end
            idx || sample_count
        end

        # Returns the IO position of a sample
        #
        # @param [Integer] sample_number the sample index in the stream
        # @return [Integer] the sample's position in the backing IO
        # @raise IndexError if the sample number is out of bounds
        def file_position_by_sample_number(sample_number)
            @index_map.fetch(sample_number * 2)
        end

        # Returns the time of a sample
        #
        # @param [Integer] sample_number the sample index in the stream
        # @return [Integer] the sample's time in the index' internal encoding
        # @raise IndexError if the sample number is out of bounds
        def internal_time_by_sample_number(sample_number)
            @index_map.fetch(sample_number * 2 + 1)
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
