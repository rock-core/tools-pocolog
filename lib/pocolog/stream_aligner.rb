require 'rbtree'

module Pocolog
    class OutOfBounds < Exception;end
    class StreamAligner
	attr_reader :use_rt
	attr_reader :use_sample_time

        attr_reader :base_time

	attr_reader :streams

        # Provided for backward compatibility only
        def count_samples
            Pocolog.warn "StreamAligner#count_samples is deprecated. Use #size instead"
            size
        end

        # Returns the number of samples one can get out of this stream aligner
        #
        # This is the sum of samples available on each of the underlying streams
        #
        # @return [Integer]
        attr_reader :size

        # The index of the current sample
        attr_reader :sample_index

        #global index for the first and last sample of the streams
        attr_reader :global_pos_last_sample
        attr_reader :global_pos_first_sample

        attr_reader :full_index

        attr_reader :time_interval      #time interval for which samples are avialable

	#The samples contained in the StreamAligner
	#Or in other words the sum over the sizes from all contained streams
	attr_reader :size
	
	# Contains whether a stream has replayed a sample 
	# since the last {seek} (or {rewind})
	attr_reader :stream_has_sample
	
	def initialize(use_rt = false, *streams)
	    @use_sample_time = use_rt == :use_sample_time
	    @use_rt  = use_rt
            @global_pos_first_sample = Hash.new
            @global_pos_last_sample = Hash.new
            @streams = streams
            @stream_state = Array.new

	    @sample_index = -1
            build_index(streams)
            rewind
        end

	# Returns the time of the last played back sample
        #
        # @return [Time]
        def time
            if entry = full_index[@sample_index]
                StreamIndex.time_from_internal(entry.time, base_time)
            end
        end

	# Tests whether reading the next sample will return something
        #
        # If {eof?} returns true, {advance} and {step} are guaranteed to return
        # nil if called.
        def eof?
	    sample_index >= size - 1
        end

        # Rewinds the stream aligner to the position before the first sample
	def rewind
	    @sample_index = -1
            nil
        end

        IndexEntry = Struct.new :time, :position_in_stream, :stream_number, :position_global

        # This method builds a composite index based on the stream index. 
	# Therefor it iterates over all streams and builds an index sample
	# every INDEX_DENSITY samples.
	# The Layout is [global_pos, [stream1 pos, stream2_pos...]]
        def build_index(streams)
	    @size = streams.inject(0) { |s, stream| s + stream.size }
	    Pocolog.info("got #{streams.size} streams with #{size} samples")
            tic = Time.now
            @base_time = streams.map { |s| s.stream_index.base_time }.min

            time_ranges = @streams.map {|s| s.time_interval(use_rt)}.flatten
            @time_interval = [time_ranges.min,time_ranges.max]

            full_index = Array.new
            streams.each_with_index do |stream, i|
                base_time_offset = stream.stream_index.base_time - base_time
                stream.stream_index.time_to_position_map.each_with_index do |time, position|
                    full_index << [time + base_time_offset, position, i]
                end
            end

            Pocolog.info "concatenated indexes in #{"%.2f" % [Time.now - tic]} seconds"
            full_index.sort!
            @full_index = full_index.each_with_index.map do |entry, position_global|
                global_pos_first_sample[entry[2]] ||= position_global
                global_pos_last_sample[entry[2]] = position_global
                IndexEntry.new(*entry, position_global)
            end
            if full_index.size != size
                raise
            end

            Pocolog.info "built index in #{"%.2f" % [Time.now - tic]} seconds"
        end
	
	def seek(pos)
	    if pos.kind_of?(Time)
		seek_to_time(pos)
	    else
		seek_to_pos(pos)
	    end
	end
	
	def seek_to_time(time, read_data = true)
            if(time < time_interval[0] || time > time_interval[1]) 
                raise RangeError, "#{time} is out of bounds valid interval #{time_interval[0]} to #{time_interval[1]}"
            end
	    
            target_time = StreamIndex.time_to_internal(time, base_time)
	    entry = @full_index.bsearch { |entry| entry.time >= target_time }
            seek_to_index_entry(entry)
	end

        # This is a private helper for {seek_to_time} and {seek_to_pos}
        def seek_to_index_entry(entry, read_data = true)
            if !entry
                @sample_index = size
                return
            end

            @sample_index = entry.position_global
            stream_idx = entry.stream_number
            @stream_state[stream_idx] = entry
            if read_data
                return stream_idx, time, single_data(stream_idx)
            else
                return stream_idx, time
            end
        end

        # Seeks to the sample whose global position is pos
        #
        # @param [Integer] pos the targetted global position
        # @param [Boolean] read_data whether the method should read the sample
        #   and return it, or only seek
        def seek_to_pos(pos, read_data = true)
            if pos < 0 || pos > size
                raise OutOfBounds, "#{pos} is out of bounds [0..#{size}]."
            end

            seek_to_index_entry(@full_index[pos])
        end
            
        def stream_index_for_name(name)
            streams.each_with_index do |s,i|
                if(s.name == name)
                    return i
                end
            end
            return nil 
        end
        
        def stream_index_for_type(name)
            stream = nil
            streams.each_with_index do |s,i|
                if(s.type_name == name)
                    raise "There exists more than one stream with type #{name}" if stream
                    stream = i
                end
            end
            stream
        end

        # call-seq:
        #   joint_stream.step => updated_stream_index, time, data
        #
        # Advances one step in the joint stream, an returns the index of the
        # updated stream as well as the time and the data sample
        #
        # The associated data sample can also be retrieved by
        # single_data(stream_idx)
        def step
            stream_idx, time = advance
            if stream_idx
                return stream_idx, time, single_data(stream_idx)
            end
        end

        # call-seq:
        #  joint_stream.advance => updated_stream_index, time 
        #
        # Advances one step in the joint stream, and returns the index of
        # the update stream as well as the time but does not encode the data sample_index
        # like step or next does
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        def advance
	    if eof?
		@sample_index = size
		return
	    end
	    
            seek_to_pos(@sample_index + 1, false)
        end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        def step_back
	    if @sample_index == 0
		@sample_index = -1
		return nil
	    end
	    
	    seek_to_pos(sample_index - 1)
        end

        # call-seq:
        #   aligner.next => time, time, [stream_index, data]
        #
        # Defined for compatibility with DataStream#next
        def next
            stream_index, time, data = step
            if stream_index
                return time, time, [stream_index, data]
            end
        end

        def pretty_print(pp)
            pp.text "next_samples:"
            pp.nest(2) do
                pp.breakable
		
                pp.seplist(streams.each_index) do |i|
		    ih = @stream_index_to_index_helpers[i]
                    if ih
                        pp.text "#{i} #{ih.time.to_f}"
                    else
                        pp.text "#{i} -"
                    end
                end
            end
            pp.breakable
            pp.text "streams:"
            pp.nest(2) do
                pp.breakable
                pp.seplist(streams.each_with_index) do |s, i|
                    if s.eof?
                        pp.text "#{i} eof"
                    else
                        pp.text "#{i} #{s.time.last.to_f}"
                    end
                end
            end
        end

        # Defined for backward compatibility with DataStream#previous
        def previous
            stream_index, time, data = step_back
            if stream_index
                return time, time, [stream_index, data]
            end
        end

        # exports all streams to a new log file 
        # if no start and end index is given all data are exported
        # otherwise the data are truncated according to the given global indexes 
        # 
        # the block is called for each sample to update a custom progress bar if the block
        # returns 1 the export is canceled
        def export_to_file(file,start_index=0,end_index=nil,&block)
            end_index ||= size - 1

            output = Pocolog::Logfiles.create(file)
            streams.each_with_index do |s|
                first_index = first_sample_pos(s)
                last_index  = last_sample_pos(s)
                # Ignore the stream if it is empty
                next if !start_index
                # Ignore the stream if there are no samples intersecting with
                # the required interval
                next if start_index >= last_index || end_index <= first_index

                stream_start_index = [start_index, first_index].max
                stream_end_index   = [end_index, last_index].min
                
                first_stream_pos = find_first_stream_sample_after(
                    stream_start_index, s)
                last_stream_pos  = find_first_stream_sample_after(
                    stream_end_index, s) || (last_index + 1)
                last_stream_pos = last_stream_pos - 1
                next if first_stream_pos >= last_stream_pos

                stream_output = output.create_stream(s.name, s.type)
                result = s.copy_to(first_stream_pos,last_stream_pos,stream_output) do |i|
                    if block
                        index +=1
                        block.call(index,number_of_samples) 
                    end
                end
                break if !result
            end
            output.close
        end

        def find_first_stream_sample_after(position_global, stream)
            # Find the time of the first sample after position_global in stream
            entry = full_index[position_global]
            time = entry.time
            stream_index = stream.stream_index
            stream_pos  = stream_index.sample_number_by_time(time)
            stream_time = stream_index.time_by_sample_number(stream_pos)
            stream_time = StreamIndex.time_to_internal(stream_time, base_time)
            if time < stream_time
                return stream_pos
            else
                # We have to check whether the sample is before or after
                # position_global and act accordingly
                stream_number = streams.index_of(stream)
                while entry && (entry.time == stream_time)
                    if entry.stream_number == stream_number && entry.position_in_stream == stream_pos
                        return stream_pos
                    end
                    entry = full_index[position_global += 1]
                end
                if entry
                    stream_pos + 1
                else
                    return
                end
            end
        end

        # Returns the global sample position of the first sample
        # of the given stream
        #
        # @param [Integer,DataStream] the stream
        # @return [nil,Integer]
        def first_sample_pos(stream)
            if !stream.kind_of?(DataStream)
                stream = streams[stream]
            end
            @global_pos_first_sample[stream] 
        end

        # Returns the global sample position of the last sample of the given
        # stream
        #
        # @param [Integer,DataStream] the stream
        # @return [nil,Integer]
        def last_sample_pos(stream)
            if !stream.kind_of?(DataStream)
                stream = streams[stream]
            end
            @global_pos_last_sample[stream]
        end

        # Returns the information necessary to read a stream's sample later
        #
        # @param [Integer] stream_idx the index of the stream
        # @return [(DataStream,Integer),nil] if there is a current sample on
        #   this stream, this is the stream and the position on the stream,
        #   suitable to be passed to {DataStream#read_one_raw_data_sample}.
        #   Otherwise, returns nil.
        #
        # @example
        #    stream_idx, time = aligner.advance
        #    @read_later = aligner.sample_info(stream_idx)
        #    ...
        #    if @read_later
        #       stream, position = *@read_later
        #       stream.read_one_raw_data_sample(position)
        #    end
        def sample_info(stream_idx)
	    if state = @stream_state[stream_idx]
		return streams[stream_idx], state.position_in_stream
	    end
        end

        # Returns the current data sample for the given stream index
	# note stream index is the index of the data stream, not the 
	# search index !
        def single_data(index, sample = nil)
            if raw = single_raw_data(index, sample)
                return Typelib.to_ruby(raw)
            end
        end

        def single_raw_data(index, sample = nil)
            stream, position = sample_info(index)
            if stream
                stream.read_one_raw_data_sample(position)
	    end
        end

        def stream_by_index(stream_idx)
            @streams[stream_idx]
        end

        def each(do_rewind = true)
            if do_rewind
                rewind
            end
            while sample = self.step
                yield(*sample)
            end
        end
    end
end

