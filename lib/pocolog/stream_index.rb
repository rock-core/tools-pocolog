require 'utilrb/module/attr_predicate'
require 'yaml'
require 'fileutils'
module Pocolog
    # This file contains the index of a data stream.
    #
    # Through this index it is possible to have an O(1) 
    # acess to the data position with an given
    # sample number.
    #
    # Time base access has a complexity of O(log N)
    #
    class StreamIndex
        attr_reader :base_time
        attr_reader :time_to_position_map

        def base_time=(value)
            @base_time ||= value
            offset = @base_time - value
            return if offset == 0
            @time_to_position_map = time_to_position_map.map do |t, i|
                [t + offset, i]
            end
            @base_time = value
        end

        def size
            @time_to_position_map.size
        end

        def initialize
            # The index holds three arrays, which associate
            # the position number of a sample in a stream
            # with the file, file position and time of the sample
            #
            # The file is encoded as an index value (rio) since 
            # pocolog accepts multifile log streams
            #
            @nr_to_position_map = Array.new()
            @base_time = nil
            @time_to_position_map = Array.new()
            @nr_to_rio = Array.new()
	end
	
	#adds a given sample header (and thus the sample) to
	#the index
	def add_sample_to_index(rio, pos, time)
	    #store the posiiton of the header of the data sample
	    @nr_to_rio << rio
	    @nr_to_position_map << pos 
            internal_time = time.tv_sec * 1_000_000 + time.tv_usec
            @base_time ||= internal_time
            @time_to_position_map << [(internal_time - @base_time), time_to_position_map.size]
	end

	# sanity check for the index, which gets called after
	# marshalling, to see if the index needs rebuilding
	def sane?
	    @nr_to_rio && @nr_to_position_map && @time_to_position_map &&
	    @nr_to_rio.size == @nr_to_position_map.size &&
	    @nr_to_rio.size == @time_to_position_map.size
	end

        def self.time_from_internal(time, base_time)
            time = time + base_time
            Time.at(time / 1_000_000, time % 1_000_000)
        end

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
            _, idx = @time_to_position_map.bsearch { |t, _| t >= sample_time }
            idx || size
	end
	
	# Expects the number of the sample that needs to be accessed 
	# and returns the position of the sample in the file
	def file_position_by_sample_number(sample_nr)
	    return @nr_to_rio[sample_nr], @nr_to_position_map[sample_nr]
	end

        def internal_time_by_sample_number(sample_nr)
	    if(sample_nr < 0 || sample_nr >= size)
		raise ArgumentError, "#{sample_nr} out of bounds"
	    end
            @time_to_position_map[sample_nr].first
        end

	# expects a sample nr and returns the time
	# of the sample
        def time_by_sample_number(sample_nr)
	    StreamIndex.time_from_internal(internal_time_by_sample_number(sample_nr), base_time)
	end

        def marshal_dump
            [@nr_to_rio.pack("n*"),
             @nr_to_position_map.pack("Q>*"),
             @base_time,
             @time_to_position_map.map(&:first).pack("Q>*")]
        end

        def marshal_load(info)
            if info.size == 4
                nr_to_rio, nr_to_position_map, base_time, time_to_position_map = *info
                @nr_to_rio = nr_to_rio.unpack("n*")
                @nr_to_position_map = nr_to_position_map.unpack("Q>*")
                @base_time = base_time
                @time_to_position_map = time_to_position_map.unpack("Q>*").each_with_index.map do |time, i|
                    [time, i]
                end
                return
            end

            nr_to_rio, nr_to_position_map, time_to_position_map = *info
            if nr_to_rio.respond_to?(:to_str)
                @nr_to_rio = nr_to_rio.unpack("n*")
                @nr_to_position_map = nr_to_position_map.unpack("Q>*")
                time_to_position_map = time_to_position_map.unpack("Q>*")
                if time_to_position_map.empty?
                else
                    # Old-new-style :( [tv_sec, tv_usec]
                    base, _ = time_to_position_map.first
                    @base_time = base
                    @time_to_position_map = time_to_position_map.each_slice(2).map { |sec, usec| (sec - base) * 1_000_000 + usec }
                end
            else
                Pocolog.warn "found an old-format index. Consider deleting all your index files to upgrade to a newer format"
                @nr_to_rio = nr_to_rio
                @nr_to_position_map = nr_to_position_map
                @time_to_position_map = time_to_position_map.map do |tv_sec, tv_usec|
                    Time.at(tv_sec, tv_usec)
                end
            end
        end
    end
end
