module Pocolog
    class StreamAligner
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt
	attr_reader :streams
        attr_reader :sample_index
	attr_reader :next_samples
	attr_reader :current_samples
        attr_reader :last_sample
        attr_reader :prev_samples

	def initialize(use_rt = false, *streams)
	    @use_rt  = use_rt
            @streams = streams
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
                index_time = index_entry.last
                positions = streams.map do |s|
                    time_range = s.time_interval(use_rt)
                    if s == reference
                        index_entry.first
                    elsif index_time < time_range[0]
                        :before
                    elsif index_time > time_range[1]
                        :after
                    else
                        s.seek(index_entry.last)
                        s.sample_index - 1
                    end
                end

                index << [positions.find_all { |p| !p.respond_to?(:to_sym) }.inject(&:+), positions]
            end
            @index = index
        end

        attr_reader :index

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

            while @sample_index != pos
                result = self.step
            end
            result
        end

        def seek_to_time(time)
            raise NotImplementedError
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
                if use_rt then header.rt
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

            @sample_index += 1
            @last_sample = min_sample = next_samples.compact.min { |s1, s2| s1.time <=> s2.time }
            if !min_sample
                return nil 
            end

            advance_stream(min_sample.stream, min_sample.stream_index)
            return min_sample.stream_index, min_sample.time,
                single_data(min_sample.stream_index)
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
            return max_sample.stream_index, max_sample.time,
                single_data(max_sample.stream_index)
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
            s.stream.data(s.header)
        end
    end
end

