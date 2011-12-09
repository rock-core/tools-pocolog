require 'pqueue'
    
module Pocolog
    class StreamAligner
	INDEX_DENSITY = 200
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt
	attr_reader :use_sample_time
	attr_reader :streams

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
	    
	    def initialize(array_pos, stream)
		@stream = stream
		@array_pos = array_pos
		@position = 0
		@time = @stream.info.index.get_time_by_sample_number(0)
	    end

	    #move stream to given position
    	    #Note that call does not do disc access
	    def set_position(pos)
		case pos
                when :before
		    @position = 0
		    @time = @stream.info.index.get_time_by_sample_number(0)
		when :after
		    @position = stream.size() - 1
		    @stream.info.index.get_time_by_sample_number(@position)
		else
		    @position = pos
		    @time = @stream.info.index.get_time_by_sample_number(pos)
		end
	    end

	    #builds an index entry for the managed stream for the given time
	    #this returns either the position, if the given time is in 
	    #the time range of the managed stream, or :after or :before
	    #Note the :after :before code can be removed
	    def build_index_entry(index_time)
		time_range = @stream.time_interval()
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
		    @time = @stream.info.index.get_time_by_sample_number(@position)
		end
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
		
		stream_index_to_index_helpers[i] = index_helpers[i] = IndexHelper.new(i, @streams[i])
		if(stream_indexes[i] == :after)
		    #remove streams that have allready been played back
		    index_helpers[i] = nil;
		else
		    index_helpers[i].set_position(stream_indexes[i])
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
 		replay_streams[s] = IndexHelper.new(s, @streams[s])
		indexes[s] = :before
	    end

	    @size = max_pos
	    
	    puts("Got #{@streams.size} streams with #{size} samples")
	    
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

		#generate a full index every INDEX_DENSITY samples
		if(pos % INDEX_DENSITY == 0)
		    stream_positions.each_index do |i|
			stream_positions[i] = :after
		    end
		    cur_time = cur_index_helper.time
		    replay_streams.to_a.each do |rh|
			stream_positions[rh[0].array_pos] = rh[0].build_index_entry(cur_time)
		    end
		    @index << [pos, stream_positions.dup]
		end

		#increase global index
		pos = pos + 1

		advance_indexes(replay_streams);

	    end
	    puts("Stream Aligner index created")

	    STDOUT.sync = old_sync_val

        end
	
	def seek(pos)
	    if pos.kind_of?(Time)
		raise "Error, seeking to time is not implemented"
	    else
		seek_to_pos(pos)
	    end
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
	    
	    index_position, stream_positions = @index[index_pos]
	    @sample_index = index_position

	    #direrence from wanted position to current position
	    diff_to_step = pos - index_position

	    #genereate a valid set of index helpers from index
	    @index_helpers = setup_index_helpers(stream_positions)
	    
	    #advance from the index position to the seeked position 
	    while(diff_to_step > 0)
		advance_indexes(@index_helpers)
		diff_to_step = diff_to_step - 1
	    end
	    
	    cur_index_helper = @index_helpers.min_key()
	    
	    #load and return data
	    rt, lg, data = cur_index_helper.stream.seek(cur_index_helper.position)
	    
	    [cur_index_helper.array_pos, lg, data]
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
	    
	    [cur_index_helper.array_pos, lg, data]
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
            STDERR.puts "WARN: StreamAligner#count_samples is deprecated. Use #size instead"
            size
        end

        # Returns the number of samples one can get out of this stream aligner
        #
        # This is the sum of samples available on each of the underlying streams
        def size
	    @size
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

