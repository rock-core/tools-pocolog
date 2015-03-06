require 'utilrb/module/attr_predicate'
require 'yaml'
require 'fileutils'
module Pocolog
    
    #
    # This file contains the index of a data stream.
    #
    # Through this index it is possible to have an O(1) 
    # acess to the data position with an given
    # sample number.
    #
    # Time base access has a complexity of O(log N)
    #
    class StreamIndex
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
            @base_time ||= time.tv_sec
	    @time_to_position_map << (time.tv_sec - @base_time) * 1_000_000 + time.tv_usec
	end

	# sanity check for the index, which gets called after
	# marshalling, to see if the index needs rebuilding
	def sane?
	    @nr_to_rio && @nr_to_position_map && @time_to_position_map &&
	    @nr_to_rio.size == @nr_to_position_map.size &&
	    @nr_to_rio.size == @time_to_position_map.size
	end

	#internal helper method
	def bSearch(arr, elem, low, high)
	    mid = low+((high-low)/2).to_i
	    if low > high
		return low
	    end
	    if elem < arr[mid]
		return bSearch(arr, elem, low, mid-1)
	    elsif elem > arr[mid]
		return bSearch(arr, elem, mid+1, high)
	    else
		return mid
	    end
	end
	
	#binary search
	def binSearch(a, x)
	    return bSearch(a, x, 0, a.length)
	end

        def time_from_internal(time)
            Time.at(time / 1_000_000 + @base_time, time % 1_000_000)
        end

        def time_to_internal(time)
            (time.tv_sec - @base_time) * 1_000_000 + time.tv_usec
        end

	#returns the sample nr of the sample before
	#the given time
	def sample_number_by_time(sample_time)
            sample_time = time_to_internal(sample_time)
	    time_map_pos = binSearch(@time_to_position_map, sample_time)

	    #we look for the sample before time x
	    if(time_map_pos > 0 && time_map_pos != @time_to_position_map.length && @time_to_position_map[time_map_pos] != sample_time )
		time_map_pos = time_map_pos - 1
	    end
	    time_map_pos
	end
	
	# Expects the number of the sample that needs to be accessed 
	# and returns the position of the sample in the file
	def file_position_by_sample_number(sample_nr)
	    return @nr_to_rio[sample_nr], @nr_to_position_map[sample_nr]
	end

	#expects a sample nr and returns the time
	#of the sample
	def get_time_by_sample_number(sample_nr)
	    if(sample_nr < 0 || sample_nr > @time_to_position_map.size() -1)
		raise ArgumentError, "#{sample_nr} out of bounds"
	    end
	    time_from_internal(@time_to_position_map[sample_nr])
	end

        def marshal_dump
            [@nr_to_rio.pack("n*"),
             @nr_to_position_map.pack("Q>*"),
             @base_time,
             @time_to_position_map.pack("Q>*")]
        end

        def marshal_load(info)
            if info.size == 4
                nr_to_rio, nr_to_position_map, base_time, time_to_position_map = *info
                @nr_to_rio = nr_to_rio.unpack("n*")
                @nr_to_position_map = nr_to_position_map.unpack("Q>*")
                @base_time = base_time
                @time_to_position_map = time_to_position_map.unpack("Q>*")
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
