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

        # A per-stream mapping from the stream index to the global position of
        # the last of this stream's samples
        #
        # @see first_sample_pos
        attr_reader :global_pos_first_sample

        # A per-stream mapping from the stream index to the global position of
        # the first of this stream's samples
        #
        # @see last_sample_pos
        attr_reader :global_pos_last_sample

        # The full aligned index
        #
        # This is a mapping from a global position in the aligned stream to
        # information about the sample at that stream
        #
        # @return [Array<IndexEntry>]
        attr_reader :full_index

        # The time of the first and last samples in the stream
        #
        # @return [(Time,Time)]
        attr_reader :time_interval
	
        def initialize(use_rt = false, *streams)
            @use_sample_time = use_rt == :use_sample_time
            @use_rt  = use_rt
            @global_pos_first_sample = Hash.new
            @global_pos_last_sample = Hash.new

            @size = 0
            @time_interval = Array.new
            @base_time = nil
            @stream_state = Array.new
            @streams = Array.new

            @sample_index = -1
            add_streams(*streams)
        end

	# Returns the time of the last played back sample
        #
        # @return [Time]
        def time
            if entry = full_index[sample_index]
                StreamIndex.time_from_internal(entry.time, base_time)
            end
        end

	# Tests whether reading the next sample will return something
        #
        # If {eof?} returns true, {#advance} and {#step} are guaranteed to return
        # nil if called.
        def eof?
	    sample_index >= size - 1
        end

        # Rewinds the stream aligner to the position before the first sample
        #
        # I.e. calling {#next} after {#rewind} would read the first sample
	def rewind
	    @sample_index = -1
            nil
        end

        IndexEntry = Struct.new :time, :stream_number, :position_in_stream, :position_global

        # Add new streams to the alignment
        def add_streams(*streams)
            return if streams.empty?

            if sample_index != -1
                current_entry = full_index[sample_index]
            end

            streams_size = streams.inject(0) { |s, stream| s + stream.size }
	    @size += streams_size
	    Pocolog.info "adding #{streams.size} streams with #{streams_size} samples"

            tic = Time.now
            if !base_time
                @base_time = streams.map { |s| s.stream_index.base_time }.compact.min
            end

            time_ranges = streams.map { |s| s.time_interval(use_rt) }.flatten.compact
            time_ranges.concat(time_interval)
            @time_interval = [time_ranges.min, time_ranges.max]

            full_index = Array.new
            (@streams + streams).each_with_index do |stream, i|
                stream.stream_index.base_time = base_time
                stream.stream_index.time_to_position_map.each do |time, position|
                    full_index << [time, i, position]
                end
            end

            Pocolog.info "concatenated indexes in #{"%.2f" % [Time.now - tic]} seconds"

            tic = Time.now
            full_index.sort!

            global_pos_first_sample.clear
            global_pos_last_sample.clear
            @full_index = full_index.each_with_index.map do |entry, position_global|
                entry = IndexEntry.new(*entry, position_global)
                global_pos_first_sample[entry.stream_number] ||= position_global
                global_pos_last_sample[entry.stream_number] = position_global
                entry
            end
            @streams.concat(streams)
            Pocolog.info "built full index in #{"%.2f" % [Time.now - tic]} seconds"

            if current_entry
                @sample_index = @full_index.
                    index do |e|
                        e.stream_number == current_entry.stream_number &&
                            e.position_in_stream == current_entry.position_in_stream
                    end
            end
        end

        # Seek at the given position or time
        #
        # @overload seek(pos, read_data = true)
        #   (see seek_to_pos)
        # @overload seek(time, read_data = true)
        #   (see seek_to_time)
	def seek(pos, read_data = true)
	    if pos.kind_of?(Time)
		seek_to_time(pos, read_data)
	    else
		seek_to_pos(pos, read_data)
	    end
	end
	
        # Seek to the first sample after the given time
        #
        # @param [Time] time the reference time
        # @param [Boolean] read_data whether the sample itself should be read or not
        # @return [(Integer,Time[,Typelib::Type])] the stream index, sample time
        #   and the sample itself if read_data is true
	def seek_to_time(time, read_data = true)
            if(time < time_interval[0] || time > time_interval[1]) 
                raise RangeError, "#{time} is out of bounds valid interval #{time_interval[0]} to #{time_interval[1]}"
            end
	    
            target_time = StreamIndex.time_to_internal(time, base_time)
	    entry = @full_index.bsearch { |e| e.time >= target_time }
            seek_to_index_entry(entry, read_data)
	end

        # Seeks to the sample whose global position is pos
        #
        # @param [Integer] pos the targetted global position
        # @param [Boolean] read_data whether the sample itself should be read or not
        # @return [(Integer,Time[,Typelib::Type])] the stream index, sample time
        #   and the sample itself if read_data is true
        def seek_to_pos(pos, read_data = true)
            if pos < 0 || pos > size
                raise OutOfBounds, "#{pos} is out of bounds [0..#{size}]."
            end

            seek_to_index_entry(@full_index[pos], read_data)
        end

        # @api private
        #
        # This is a private helper for {seek_to_time} and {seek_to_pos}. It
        # seeks the stream aligner to the given global sample
        #
        # @param [IndexEntry] entry index entry of the global sample we want to
        #   seek to
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

        # Return the stream object at the given stream index
        #
        # @param [Integer] stream_idx
        # @return [DataStream]
        def stream_by_index(stream_idx)
            @streams[stream_idx]
        end

        # Returns the stream index for the given stream
        def stream_index_for_stream(stream)
            streams.index(stream)
        end
            
        # Returns the stream index of the stream with this name
        #
        # @param [String] name
        # @return [Integer,nil]
        def stream_index_for_name(name)
            streams.each_with_index do |s,i|
                if(s.name == name)
                    return i
                end
            end
            return nil 
        end
        
        # Returns the stream index of the stream whose type has this name
        #
        # @param [String] name
        # @return [Integer,nil]
        # @raise [ArgumentError] if more than one stream has this type
        def stream_index_for_type(name)
            stream = nil
            streams.each_with_index do |s,i|
                if(s.type_name == name)
                    raise ArgumentError, "There exists more than one stream with type #{name}" if stream
                    stream = i
                end
            end
            stream
        end

        # Advances one step in the joint stream, and returns the index of the
        # updated stream as well as the time and the data sample
        #
        # The associated data sample can also be retrieved by
        # single_data(stream_idx)
        #
        # @return [(Integer,Time,Typelib::Type)]
        # @see advance
        def step
            stream_idx, time = advance
            if stream_idx
                return stream_idx, time, single_data(stream_idx)
            end
        end

        # Advances one step in the joint stream, and returns the index of the
        # update stream as well as the time but does not decode the data
        # sample_index like {#step} or {#next} does
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        #
        # @return [(Integer,Time)]
        # @see step
        def advance
	    if eof?
		@sample_index = size
		return
	    end
	    
            seek_to_pos(@sample_index + 1, false)
        end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream, its time as well as the sample data
        #
        # @return [(Integer,Time,Typelib::Type)]
        def step_back
	    if @sample_index == 0
		@sample_index = -1
		return nil
	    end
	    
	    seek_to_pos(sample_index - 1)
        end

        # Defined for compatibility with DataStream#next
        #
        # Goes one sample further and returns the sample's logical and real
        # times as well as the stream index and the sample data
        #
        # @return [(Time,Time,(Integer,Typelib::Type)),nil]
        def next
            stream_index, time, data = step
            if stream_index
                return time, time, [stream_index, data]
            end
        end

        # Defined for compatibility with DataStream#previous
        #
        # Goes back one sample and returns the sample's logical and real
        # times as well as the stream index and the sample data
        #
        # @return [(Time,Time,(Integer,Typelib::Type)),nil]
        def previous
            stream_index, time, data = step_back
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

        # exports all streams to a new log file 
        # if no start and end index is given all data are exported
        # otherwise the data are truncated according to the given global indexes 
        # 
        # the block is called for each sample to update a custom progress bar if the block
        # returns 1 the export is canceled
        def export_to_file(file,start_index=0,end_index=size,&block)
            output = Pocolog::Logfiles.create(file)
            streams.each do |s|
                stream_start_index = first_sample_pos(s)
                stream_end_index  = last_sample_pos(s) + 1
                # Ignore the stream if it is empty
                next if !stream_start_index
                # Ignore the stream if there are no samples intersecting with
                # the required interval
                next if start_index >= stream_end_index || end_index <= stream_start_index

                stream_start_index = [start_index, stream_start_index].max
                stream_end_index   = [end_index, stream_end_index].min
                
                first_stream_pos = find_first_stream_sample_at_or_after(
                    stream_start_index, s)
                last_stream_pos  = find_first_stream_sample_at_or_after(
                    stream_end_index, s)
                next if first_stream_pos == last_stream_pos

                index = 0
                number_of_samples = stream_end_index-stream_start_index+1
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

        # Returns the stream-local index of the first sample that is either at
        # or just after the given global position
        #
        # @param [Integer] position_global a global position
        # @param [DataStream] stream the data stream
        def find_first_stream_sample_at_or_after(position_global, stream)
            if !(entry = full_index[position_global])
                return stream.size
            end

            stream_number = stream_index_for_stream(stream)
            if entry.stream_number == stream_number
                return entry.position_in_stream
            end

            find_first_stream_sample_after(position_global, stream)
        end

        # Returns the stream-local index of the first sample strictly after the
        # sample at the given global position
        #
        # @param [Integer] position_global a global position
        # @param [DataStream] stream the data stream
        def find_first_stream_sample_after(position_global, stream)
            if !(entry = full_index[position_global])
                return stream.size
            end

            stream_number = stream_index_for_stream(stream)

            # First things first, if entry is a sample of stream, we just have
            # to go forward by one
            if entry.stream_number == stream_number
                return entry.position_in_stream + 1
            end

            # Otherwise, we need to search in the stream
            time  = entry.time
            search_pos = stream.stream_index.sample_number_by_internal_time(time)
            if search_pos == stream.size
                return search_pos
            end

            # If the sample we found has the same time than the entry at
            # position_global, We now have to figure out whether it is before or
            # after position global
            #
            # We do a linear search in all samples that have the same time than
            # the reference time. This basically assumes that you don't have a
            # million samples with the same time. I believe it fair.
            search_time = stream.stream_index.internal_time_by_sample_number(search_pos)
            if search_time != time
                return search_pos
            end

            while entry && entry.time == time
                if entry.stream_number == stream_number
                    return entry.position_in_stream
                end
                entry = @full_index[position_global += 1]
            end
            return search_pos + 1
        end

        # Returns the global sample position of the first sample
        # of the given stream
        #
        # @param [Integer,DataStream] the stream
        # @return [nil,Integer]
        def first_sample_pos(stream)
            if stream.kind_of?(DataStream)
                stream = streams.index(stream)
            end
            @global_pos_first_sample[stream] 
        end

        # Returns the global sample position of the last sample of the given
        # stream
        #
        # @param [Integer,DataStream] the stream
        # @return [nil,Integer]
        def last_sample_pos(stream)
            if stream.kind_of?(DataStream)
                stream = streams.index(stream)
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
        #
        # @param [Integer] index index of the stream
        # @param [Typelib::Type,nil] sample if given, the sample will be decoded
        #   in this object instead of creating a new one
        # @return [Object,nil]
        def single_data(index, sample = nil)
            if raw = single_raw_data(index, sample)
                return Typelib.to_ruby(raw)
            end
        end

        # Returns the current raw data sample for the given stream index
	# note stream index is the index of the data stream, not the 
	# search index !
        #
        # @param [Integer] index index of the stream
        # @param [Typelib::Type,nil] sample if given, the sample will be decoded
        #   in this object instead of creating a new one
        # @return [Typelib::Type]
        def single_raw_data(index, sample = nil)
            stream, position = sample_info(index)
            if stream
                stream.read_one_raw_data_sample(position)
	    end
        end

        # Enumerate all samples in this stream
        #
        # @param [Boolean] do_rewind whether {#rewind} should be called first
        # @yieldparam [Integer] stream_idx the stream in which the sample is
        #   contained
        # @yieldparam [Time] time the stream time
        # @yieldparam [Object] sample the sample itself
        def each(do_rewind = true)
            return enum_for(__method__, do_rewind) if !block_given?
            if do_rewind
                rewind
            end
            while sample = self.step
                yield(*sample)
            end
        end
    end
end

