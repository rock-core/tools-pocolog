module Pocosim
    class StreamAligner
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt

	attr_reader :streams
        attr_reader :current_streams
	attr_reader :current_samples
	attr_reader :next_samples
        attr_reader :prev_samples
        attr_reader :current_pos
        attr_reader :eof
        attr_reader :forward_replay

	def initialize(use_rt = false, *streams)
	    @use_rt  = use_rt
	    @streams = streams

            rewind
	end

        attr_reader :stream_time
        attr_reader :time

	def rewind
            @current_samples = Array.new
            @next_samples    = Array.new
            @current_streams = Array.new
            @prev_samples = Array.new
            @current_pos = -1
            @eof = false;
            @forward_replay = true

            min_stream, min_time = nil
            streams.each_with_index do |s, i|
		header = s.rewind
                return if !header

                time = s.time
                time = if use_rt then time.first
                       else time.last
                       end

                if !min_time || min_time > time
                    min_time = time
                    min_stream = next_samples.size
                end

		next_samples    << StreamSample.new(time, header.dup, s, i)
                current_streams << s
	    end
            nil
        end

        def decrement_stream(s,i)
            if !prev_samples[i]
              raise "Cannot decrement_stream #{s.name}. Beginning is reached"
            end

            next_samples[i] = current_samples[i]
            current_samples[i] = prev_samples[i]
	    @time = current_samples[i].time

            sample = s.previous
            header = s.data_header
	    if !sample || !header
		prev_samples[i] = nil
		return current_samples[i]
	    end
            @stream_time =
	        if use_rt then header.rt
                else header.lg
                end

            prev_samples[i] = StreamSample.new self.stream_time, header.dup, s, i
            current_samples[i]
        end

	def advance_stream(s, i)
            if !next_samples[i]
              raise "Cannot advance stream #{s.name}. End is reached"
            end

            prev_samples[i] = current_samples[i]
	    current_samples[i] = next_samples[i]
            @time = current_samples[i].time

	    header = s.advance
	    if !header
		next_samples[i] = nil
		return current_samples[i]
            end
            @stream_time =
		if use_rt then header.rt
		else header.lg
		end

	    next_samples[i] = StreamSample.new self.stream_time, header.dup, s, i
            current_samples[i]
	end

        # call-seq:
        #   joint_stream.step => updated_stream_index, time, data
        #
        # Advances one step in the joint stream, an returns the index of the
        # updated stream as well as the time and the data sample
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        def step	   
             
          #check if play direction has changed 
            if @forward_replay == false
              current_samples.compact.each do |sample|
                sample.stream.advance if next_samples[sample.stream_index] != nil 
                sample.stream.advance if prev_samples[sample.stream_index] != nil
              end
             @forward_replay = true
            end
	   
            min_sample = next_samples.compact.min { |s1, s2| s1.time <=> s2.time }
            if !min_sample
              @eof = true;
              return nil 
            end
            @current_pos += 1
	    advance_stream(min_sample.stream, min_sample.stream_index)
	    return min_sample.stream_index, min_sample.time, single_data(min_sample.stream_index)
        end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)

        def step_back
           #check if play direction has changed 
            if @forward_replay == true
              current_samples.compact.each do |sample|
                next if current_samples[sample.stream_index] == nil
                sample.stream.previous if next_samples[sample.stream_index] != nil 
                sample.stream.previous if prev_samples[sample.stream_index] != nil
              end
             @forward_replay = false
            end

            max_sample = prev_samples.compact.max{ |s1, s2| s1.time <=> s2.time }
            return nil if !max_sample
            @eof = false;
            @current_pos -=1
	    decrement_stream(max_sample.stream, max_sample.stream_index)
	    return max_sample.stream_index, max_sample.time, single_data(max_sample.stream_index)
        end

        def count_samples
            number_of_samples = 0;
            @streams.each do |s|
              number_of_samples += s.size
            end
            return number_of_samples
        end

        # Returns the current data sample for the given stream index
        def single_data(index)
            s = current_samples[index]
            s.stream.data(s.header)
        end
    end
end

