module Pocolog
    require 'priority_queue'

    class OutOfBounds < Exception;end
    class StreamAligner
	
	class MultiStreamIndex
	    attr_reader :stream_positions
	    attr_reader :index_time
	    attr_reader :position
	    
	    def initialize(pos, time, nr_streams)
		@position = pos
		@index_time = time
		@stream_positions = Array.new(nr_streams)
	    end
	end
	
	INDEX_DENSITY = 200
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt
	attr_reader :use_sample_time
	attr_reader :streams

        #global index for the first and last sample of the streams
        attr_reader :global_pos_last_sample
        attr_reader :global_pos_first_sample

	#sample_index contains the global index in respect to
	#the field @index
        attr_reader :sample_index

        attr_reader :index
        attr_reader :time_interval      #time interval for which samples are avialable

	#The samples contained in the StreamAligner
	#Or in other words the sum over the sizes from all contained streams
	attr_reader :size
	
	#helper classes, that are used for seeking and replaying
	attr_reader :index_helpers
	#map for mapping from stream_index to index helpers
	attr_reader :stream_index_to_index_helpers
	#contains weather a stream has allready replayed a sample 
	#in respect to the sample_index 
	attr_reader :stream_has_sample
	
	def initialize(use_rt = false, *streams)
	    @use_sample_time = use_rt == :use_sample_time
	    @use_rt  = use_rt
            #remove empty streams 
            raise ArgumentError.new("Empty streams are not supported [" + streams.find{|stream| stream.empty?}.name + "].") if streams.find{|stream| stream.empty?}
            @global_pos_first_sample = Hash.new
            @global_pos_last_sample = Hash.new
            @streams = streams
	    @stream_has_sample = Array.new
	    @stream_index_to_index_helpers = Array.new
            time_ranges = @streams.map {|s| s.time_interval(use_rt)}.flatten
            @time_interval = [time_ranges.min,time_ranges.max]
            build_index
            rewind
        end

	#returns the time of the last played back sample
        def time
            if !index_helpers.empty?
                index_helpers.min_key().time
            end
        end

	#checks if the end of all streams was reached
        def eof?
	    sample_index > (size - 2)
        end

	#rewinds the stream aligner to the position
	#before the first sample
	def rewind
	    self.seek_to_pos(0)
	    @sample_index = -1
            nil
        end

	#helper class that is used to manipulate
	#the index of an stream in an efficent way
	class IndexHelper
	    #current position of the stream that is handeled
	    attr_accessor :position
	    #current time of the stream that is handeled
	    attr_accessor :time
	    #the stream that if handeled by this class
	    attr_accessor :stream
	    #position of the stream in the streams array in the Stream Aligner
	    attr_accessor :array_pos
	    
	    #HACK if this is set to true, we will decode every sample
	    #and use the time field of the sample as time
	    attr_reader :use_sample_time
	    
	    #this needs to be calculated here, as it changes depending
	    #on weather sample or stream time is used
	    attr_reader :time_range

	    def initialize(array_pos, stream, use_sample_time = nil)
		@stream = stream
		@array_pos = array_pos
		@position = 0
		@use_sample_time = use_sample_time
		@time_range = []
		#order is important here as this MAY seek to the end of stream
		last_time = getTime(@stream.size() - 1)
		@time_range[1] = last_time

		#and this MAY seeks back to the beginning
		@time = getTime(0)
		@time_range[0] = @time
	    end

	    def getTime(sampleNr)
		time = nil
		if(@use_sample_time)
		    #move stream to sample
		    @stream.seek(sampleNr, false)
		    time = @stream.sub_field('time')
		    if(!time)
			#puts("Stream #{@stream.name} has no time field falling back to stream time")
			#sample does not have sample time, disable this
			#and fall back to stream time_interval
			@use_sample_time = false
			time = getTime(sampleNr)
		    end
		else
		    time = @stream.info.index.get_time_by_sample_number(sampleNr)
		end
		time
	    end
	    
	    #move stream to given position
    	    #Note that call does not do disc access
	    def set_position(pos)
		case pos
                when :before
		    @position = 0
		    @time = getTime(0)
		when :after
		    @position = stream.size() - 1
		    @time = getTime(@position)
		else
		    @position = pos
		    @time = getTime(pos)
		end
	    end

	    #builds an index entry for the managed stream for the given time
	    #this returns either the position, if the given time is in 
	    #the time range of the managed stream, or :after or :before
	    #Note the :after :before code can be removed
	    def build_index_entry(index_time)
		if index_time < time_range[0]
		    :before
		elsif index_time > time_range[1]
		    :after
		else
		    @position
		end
	    end

	    #advance on stream_index by one.
	    #Note that call does not do disc access
	    def next()
		if(eof?)
		    nil
		else
		    @position = @position + 1
		    @time = getTime(@position)
		end
	    end

            #returns true if the current position is
            #the first sample
            def first?
                position == 0
            end
	    
	    #checks weather the end of stream was reached
	    def eof?
		if(@position < @stream.size() - 1)
		    false
		else
		    true
		end
	    end
	end

	#this function sets up the index helpers to
	#be in the state of the given stream indexes
	def setup_index_helpers(stream_indexes)	    
	    index_helpers = Array.new(stream_indexes.size)
	    stream_indexes.each_index do |i|
		#mark is sample has stream or not
		if(stream_indexes[i] == :before)
		    @stream_has_sample[i] = false
		else
		    @stream_has_sample[i] = true
		end
		
		stream_index_to_index_helpers[i] = index_helpers[i] = IndexHelper.new(i, @streams[i], @use_sample_time)
		#set index helper correct position in stream
		index_helpers[i].set_position(stream_indexes[i])
		if(stream_indexes[i] == :after)
		    #remove streams that have allready been played back
		    #from playback list
		    index_helpers[i] = nil;
		end
	    end
	   
	    #remove nil helpers
	    index_helpers.compact!
	    
	    pq = PriorityQueue.new()
	    
	    index_helpers.each do |helper|
		pq.push(helper, helper.time)
	    end
	    
	    pq
	end	
	
	def advance_indexes(index_helpers)
	    @sample_index = @sample_index + 1

	    #cur_index_helper represents the last played back sample
	    cur_index_helper = index_helpers.min_key()

	    #mark stream as played back
	    @stream_has_sample[cur_index_helper.stream.index] = true

	    #check if we can advance current stream
	    if(cur_index_helper.next())
		#time advance, change priority of stream
		index_helpers.change_priority(cur_index_helper, cur_index_helper.time)
	    else
		index_helpers.delete_min()
	    end
	end
	
        # This method builds a composite index based on the stream index. 
	# Therefor it iterates over all streams and builds an index sample
	# every INDEX_DENSITY samples.
	# These samples are stored in @index
	# The Layout is [global_pos, [stream1 pos, stream2_pos...]]
        def build_index
	    @index = Array.new
	    @sample_index = 0
	    
	    max_pos = 0
	    replay_streams = Array.new
	    indexes = Array.new
	    
	    #all streams start at 0
	    stream_positions = Array.new
	    @streams.each_index do |s| 
		stream_positions[s] = 0
		max_pos += @streams[s].size
 		replay_streams[s] = IndexHelper.new(s, @streams[s], @use_sample_time)
		indexes[s] = :before
	    end

	    @size = max_pos
	    
	    Pocolog.info("Got #{@streams.size} streams with #{size} samples")
	    
	    pos = 0

	    pq = PriorityQueue.new()
	    
	    replay_streams.each do |helper|
		pq.push(helper, helper.time)
	    end
	    
	    replay_streams = pq
	    
	    percentage = nil
	    old_sync_val = STDOUT.sync
	    STDOUT.sync = true
	    
	    #iterate over all streams and generate the index
	    while(pos < max_pos)
		new_percentage = pos * 100 / max_pos
		if(new_percentage != percentage)
		   percentage = new_percentage
		   print("\r#{percentage}% indexed")
		end
		
		cur_index_helper = replay_streams.min_key
		if(!cur_index_helper)
		    raise("Internal error, no stream available for playback, but not all samples were played back")
		end

                #save the global pos for the first and last sample of each stream
                if cur_index_helper.first?
                    @global_pos_first_sample[cur_index_helper.stream] = pos
                elsif cur_index_helper.eof?
                    @global_pos_last_sample[cur_index_helper.stream] = pos
                end

		#generate a full index every INDEX_DENSITY samples
		if(pos % INDEX_DENSITY == 0)
		    index_sample = MultiStreamIndex.new(pos, cur_index_helper.time, stream_positions.size)
		    stream_positions.each_index do |i|
			index_sample.stream_positions[i] = :after
		    end
		    cur_time = cur_index_helper.time
		    replay_streams.to_a.each do |rh|
			index_sample.stream_positions[rh[0].array_pos] = rh[0].build_index_entry(cur_time)
		    end
		    @index << index_sample
		end

		#increase global index
		pos = pos + 1

		advance_indexes(replay_streams);

	    end
	    Pocolog.info("Stream Aligner index created")

	    STDOUT.sync = old_sync_val

        end
	
	def seek(pos)
	    if pos.kind_of?(Time)
		seek_to_time(pos)
	    else
		seek_to_pos(pos)
	    end
	end
	
	def seek_to_time(time)
	   if(time < time_interval[0] || time > time_interval[1]) 
                raise RangeError, "#{time} is out of bounds valid interval #{time_interval[0]} to #{time_interval[1]}"
            end
	    
	    searched_index = @index[0]
	    #stupid and slow implementation for now
	    @index.each do |index_sample|
		if(index_sample.index_time > time)
		    break;
		end
		searched_index = index_sample
	    end
	    
	    #searched_index points now to the index before the time
	    #now look for the sample position	    
	    @sample_index = searched_index.position

	    #genereate a valid set of index helpers from index
	    @index_helpers = setup_index_helpers(searched_index.stream_positions)

	    #advance index to the sample BEFORE 'time'
	    while(@index_helpers.min_key().time < time)
		advance_indexes(@index_helpers)
	    end

	    cur_index_helper = @index_helpers.min_key()

	    #load and return data
	    rt, lg, data = cur_index_helper.stream.seek(cur_index_helper.position)
	    
	    [cur_index_helper.array_pos, cur_index_helper.time, data]	    
	end

	#seeks to the given position
	#note that this method does only disk io
	#when loading the sample at pos
        def seek_to_pos(pos)
            if pos < 0 || pos > size
                raise OutOfBounds, "#{pos} is out of bounds"
            end

	    #position of index before pos
	    index_pos = Integer(pos / INDEX_DENSITY)
	    
	    index_sample = @index[index_pos]
	    @sample_index = index_sample.position

	    #direrence from wanted position to current position
	    diff_to_step = pos - index_sample.position

	    #genereate a valid set of index helpers from index
	    @index_helpers = setup_index_helpers(index_sample.stream_positions)
	    
	    #advance from the index position to the seeked position 
	    while(diff_to_step > 0)
		advance_indexes(@index_helpers)
		diff_to_step = diff_to_step - 1
	    end
	    
	    cur_index_helper = @index_helpers.min_key()
	    
	    #load and return data
	    rt, lg, data = cur_index_helper.stream.seek(cur_index_helper.position)
	    
	    [cur_index_helper.array_pos, cur_index_helper.time, data]
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
	    if(eof?)
		@sample_index = size
		return nil
	    end
	    
	    if(@sample_index == -1)
		@sample_index = 0
	    else
		advance_indexes(@index_helpers)
	    end
	    
	    cur_index_helper = @index_helpers.min_key()
	    
	    #load and return data
	    rt, lg, data = cur_index_helper.stream.seek(cur_index_helper.position)
	    
	    [cur_index_helper.array_pos, cur_index_helper.time, data]
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
	    if(eof?)
		return nil
	    end
	    
	    if(@sample_index == -1)
		@sample_index = 0
	    else
		advance_indexes(@index_helpers)
	    end
	    
	    cur_index_helper = @index_helpers.min_key()
	    
            [cur_index_helper.array_pos, cur_index_helper.time]
         end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        def step_back
	    if(@sample_index <= 0)
		@sample_index = -1
		return nil
	    end
	    
	    #could be more performant, but does the job well
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

        # Provided for backward compatibility only
        def count_samples
            Pocolog.warn "StreamAligner#count_samples is deprecated. Use #size instead"
            size
        end

        # Returns the number of samples one can get out of this stream aligner
        #
        # This is the sum of samples available on each of the underlying streams
        def size
	    @size
        end

        # exports all streams to a new log file 
        # if no start and end index is given all data are exported
        # otherwise the data are truncated according to the given global indexes 
        # 
        # the block is called for each sample to update a custom progress bar if the block
        # returns 1 the export is canceled
        def export_to_file(file,start_index=0,end_index=nil,&block)
            #save current pos 
            index = 0
            old_index = @sample_index 
            end_index = size-1 if !end_index
            seek_to_pos(end_index)

            #save all end positions
            end_positions = Hash.new
            @index_helpers.each do |helper,_|
                end_positions[helper.stream.name] = helper.position
            end

            seek_to_pos(start_index)
            number_of_samples = end_index-start_index+1

            #we have to create the log file manually because we do not want to 
            #use the automatically applied file name logic
            output = Pocolog::Logfiles.new(Typelib::Registry.new)
            output.new_file(file)
        
            #copy all streams which have samples inside the given interval
            @index_helpers.each do |helper,_|
                next if first_sample_pos(helper.stream) > end_index
                end_pos = end_positions[helper.stream.name]
                end_pos = helper.stream.size-1 if !end_pos
                stream_output = output.stream(helper.stream.name,helper.stream.type,true)
                result = helper.stream.copy_to(helper.position,end_pos,stream_output) do |i|
                    if block
                        index +=1
                        block.call(index,number_of_samples) 
                    end
                end
                break if !result
            end
            output.close

            #return to old pos
            if old_index < 0
                rewind
            else
                seek_to_pos(old_index)
            end
        end

        #returns the global sample position of the first sample
        #of the given stream
        def first_sample_pos(stream)
            @global_pos_first_sample[stream] 
        end

        #returns the global sample position of the last sample
        #of the given stream
        def last_sample_pos(stream)
            @global_pos_last_sample[stream]
        end

        # Returns the current data sample for the given stream index
	# note stream index is the index of the data stream, not the 
	# search index !
        def single_data(index)
	    if(@stream_has_sample[index])
		helper = @stream_index_to_index_helpers[index]
		rt, lg, data = helper.stream.seek(helper.position)
		data
	    else
		nil
	    end
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

