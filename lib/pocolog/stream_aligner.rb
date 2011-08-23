module Pocolog
    class StreamAligner
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt
	attr_reader :use_sample_time
	attr_reader :streams
        attr_reader :sample_index
	attr_reader :next_samples
	attr_reader :current_samples
        attr_reader :last_sample
        attr_reader :prev_samples
        attr_reader :index
        attr_reader :time_interval      #time interval for which samples are avialable

	def initialize(use_rt = false, *streams)
	    @use_sample_time = use_rt == :use_sample_time
	    @use_rt  = use_rt
            @streams = streams 
            time_ranges = @streams.map {|s| s.time_interval(use_rt)}.flatten
            @time_interval = [time_ranges.min,time_ranges.max]
            build_index
            rewind
        end

        def time
            if last_sample
                last_sample.time
            end
        end

        def eof?
            @next_samples.compact.empty?
        end

	def rewind
            @prev_samples    = Array.new
            @next_samples    = Array.new
            @current_samples = prev_samples
            @last_sample     = nil
            @sample_index    = -1

            streams.each_with_index do |s, i|
                s.rewind
                advance_stream(s, i)
            end
            nil
        end

        # This method builds a composite index based on the stream index. What
        # it does is use the most fine-grained stream as a reference base, and
        # sample in time at each of this stream's index entry. Then, record the
        # sample index for each of the separate streams and compute the global
        # index
        def build_index
            reference = streams.max do |s1, s2|
                s1.info.index.size <=> s2.info.index.size
            end
            reference_index = reference.info.index

            index = Array.new
            reference_index.each do |index_entry|
                index << build_index_entry(index_entry, reference)
            end
            @index = index
        end

        #generates an entry for the global index, based on a time
        #object or an stream index_entry
        #if a reference stream is provided it is
        #assumed that index_entry is an index entry of 
        #the reference stream
        def build_index_entry(index_entry,reference_stream=nil)
          glob_index = -1
          index_time = index_entry.is_a?(Array) ? index_entry.last : index_entry
          positions = streams.map do |s|
              time_range = s.time_interval(use_rt)
              if s == reference_stream
                  glob_index += index_entry.first+1
                  index_entry.first
              elsif index_time < time_range[0]
                  :before
              elsif index_time > time_range[1]
                  glob_index += s.size
                  :after
              else
                  s.seek(index_time)
                  glob_index += s.sample_index+1
                  s.sample_index
              end
          end
          [glob_index, positions]
        end

        def preseek(entry)
            # Forcefully switch to forward play direction
            @current_samples = prev_samples
            return if !entry
            streams.each_with_index do |s, i|
                positions = entry.last
                case positions[i]
                when :before
                    s.rewind
                    s.advance
                    prev_samples[i] = nil
                    next_samples[i] = create_stream_sample(s, i)
                when :after
                    s.last
                    prev_samples[i] = create_stream_sample(s, i)
                    next_samples[i] = nil
                    s.next
                else
                    if positions[i] == 0
                        s.rewind
                        s.next
                    else
                        s.seek(positions[i])
                    end
                    next_samples[i] = create_stream_sample(s, i)
                    advance_stream(s, i)
                end
            end
            @sample_index = entry[0]
            @last_sample = current_samples.compact.
                max { |s1, s2| s1.time <=> s2.time }
        end

        def seek_to_pos(pos)
            if pos < 0 || pos > size
                raise OutOfBounds, "#{pos} is out of bounds"
            end

            if pos < index.first[0]
                rewind
            elsif pos > index.last[0]
                entry = index.last
            elsif index.size == 1
                entry = index.first
            else
                entry, _ = index.each_cons(2).find do |before, after|
                    after[0] > pos
                end
            end
            preseek(entry)
            while @sample_index < pos
                self.advance
            end
            [@last_sample.stream_index, @last_sample.time,
                      single_data(@last_sample.stream_index)] if @last_sample
        end
            
        
        def get_stream_index_for_name(name)
            streams.each_with_index do |s,i|
                if(s.name == name)
                    return i
                end
            end
            return NIL
        end
        
        def get_stream_index_for_type(name)
            streams.each_with_index do |s,i|
                if(s.type_name == name)
                    return i
                end
            end
            return NIL
        end

        #seeks all streams to a sample which logical time is not greater than the given
        #time. If this is not possible the stream will be re winded.
        def seek_to_time(time)
          raise ArgumentError "a time object is expected" if !time.is_a?(Time) 
          if time < time_interval.first || time > time_interval.last
            pp "Time is not in bounds, NOT: #{time_interval.first} < #{time} < #{time_interval.last}"
            raise OutOfBounds 
          end
          #we can calc the  preseek settings we do not have to cache them like for sample based index
          entry = build_index_entry(time)
          preseek(entry)

          while @next_samples.compact.min { |s1, s2| s1.time <=> s2.time }.time < time
              self.advance
          end
          [@last_sample.stream_index, @last_sample.time,
            single_data(@last_sample.stream_index)] if @last_sample
        end

        def seek(pos_or_time)
            if pos_or_time.kind_of?(Integer)
                seek_to_pos(pos_or_time)
            else
                seek_to_time(pos_or_time)
            end
        end

        # True if the last play operation was going forward
        def playing_forward?;  current_samples.object_id == prev_samples.object_id end
        # True if the last play operation was going backward
        def playing_backward?; current_samples.object_id == next_samples.object_id end

        # Create a StreamSample instance that represents the current state of
        # the given stream
        def create_stream_sample(s, i)
            header = s.data_header
            stream_time =
		if use_sample_time then s.data.time
		elsif use_rt then header.rt
                else header.lg
                end
            StreamSample.new stream_time, header.dup, s, i
        end

        # When playing in the backward direction, prev_samples is the lookahead,
        # next_samples the current stream
        def decrement_stream(s,i)
            next_samples[i], prev_samples[i] = prev_samples[i], nil
            if s.previous
                prev_samples[i] = create_stream_sample(s, i)
            end
        end

        # When playing in the forward direction, next_samples is the lookahead
        # and prev_samples the current stream
	def advance_stream(s, i)
            prev_samples[i], next_samples[i] = next_samples[i], nil
	    if s.advance
                next_samples[i] = create_stream_sample(s, i)
            end
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
          index,time = advance 
          return if !index
          [index,time,single_data(index)] 
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
            return if sample_index == size

            # Check if we are changing the replay direction. If it is the case,
            # we have to throw the first sample away as we are always reading
            # with a one-sample lookahead
            if playing_backward?
                streams.each_with_index { |s, i| s.next }
                if last_sample
                    advance_stream(last_sample.stream, last_sample.stream_index)
                end
                @current_samples = prev_samples
            end

            @last_sample = min_sample = next_samples.compact.min { |s1, s2| s1.time <=> s2.time }
            @sample_index += 1
            if !min_sample
                return nil 
            end

            advance_stream(min_sample.stream, min_sample.stream_index)
            [min_sample.stream_index, min_sample.time]
         end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        def step_back
            return if sample_index == -1

            # Check if we are changing the replay direction. If it is the case,
            # we have to throw the first sample away as we are always reading
            # with a one-sample lookahead
            if playing_forward?
                streams.each_with_index { |s, i| s.previous }
                if last_sample
                    decrement_stream(last_sample.stream, last_sample.stream_index)
                end
                @current_samples = next_samples
            end

            @sample_index -= 1
            @last_sample = max_sample = prev_samples.compact.max { |s1, s2| s1.time <=> s2.time }
            if !max_sample
                return nil
            end

            decrement_stream(max_sample.stream, max_sample.stream_index)
            [max_sample.stream_index, max_sample.time,single_data(max_sample.stream_index) ]
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
            pp.text "playing " + (playing_forward? ? "forward" : "backward")
            pp.breakable
            pp.text "prev_samples:"
            pp.nest(2) do
                pp.breakable
                pp.seplist(prev_samples.each_with_index) do |s, i|
                    if s
                        pp.text "#{i} #{s.time.to_f}"
                    else
                        pp.text "#{i} -"
                    end
                end
            end
            pp.breakable
            pp.text "next_samples:"
            pp.nest(2) do
                pp.breakable
                pp.seplist(next_samples.each_with_index) do |s, i|
                    if s
                        pp.text "#{i} #{s.time.to_f}"
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
            streams.inject(0) { |result, s| result += s.size }
        end

        # Returns the current data sample for the given stream index
        def single_data(index)
            s = current_samples[index]
            s.stream.data(s.header) if s
        end
    end
end

