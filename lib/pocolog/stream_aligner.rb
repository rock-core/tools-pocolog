module Pocolog
    class StreamAligner
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt
	attr_reader :streams
        attr_reader :current_pos
        attr_reader :current_streams
	attr_reader :next_samples
	attr_reader :current_samples
        attr_reader :prev_samples

	def initialize(use_rt = false, *streams)
	    @use_rt  = use_rt
            @streams = streams
            rewind
        end

        attr_reader :time

        def eof?
            @next_samples.compact.empty?
        end

	def rewind
            @current_streams = Array.new
            @prev_samples    = Array.new
            @current_samples = Array.new
            @next_samples    = Array.new
            @current_pos = -1

            streams.each_with_index do |s, i|
		header = s.rewind
                if !header   
                    next_samples << nil
                else    
                  time = s.time
                  time = if use_rt then time.first
                       else time.last
                       end
		  next_samples    << StreamSample.new(time, header.dup, s, i)
                end
                current_streams << s
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

            index = Array.new
            reference.info.index.each do |index_entry|
                positions = streams.map do |s|
                    if s == reference
                        index_entry.first
                    else
                        s.seek(index_entry.last)
                    end
                end

                index << [positions.inject(&:+), positions]
            end
            @index = index
        end

        def seek_to_pos(pos)
            if pos < 0 || pos > size
                raise "#{pos} is out of bounds"
            end

            # For the aligner to function properly, we need to initialize the
            # prev_samples and current_samples array. Seek to one before the
            # target position to do that
            target_pos = pos - 1
            if target_pos > index.last[0]
                entry = index.last
            else
                entry, _ = index.each_cons(2).find do |before, after|
                    after[0] > target_pos
                end
            end
            streams.each_with_index do |s, i|
                s.seek(entry[i + 1])
            end
        end

        def seek_to_time(time)
        end

        def seek(pos_or_time)
            if pos_or_time.kind_of?(Integer)
                seek_to_pos(pos_or_time)
            else
                seek_to_time(pos_or_time)
            end
        end

        def decrement_stream(s,i)
            next_samples[i], prev_samples[i] =
                prev_samples[i], nil

            s.previous
            header = s.data_header
	    if header
                stream_time =
                    if use_rt then header.rt
                    else header.lg
                    end
                prev_samples[i] = StreamSample.new stream_time, header.dup, s, i
            end
            current_samples[i] = next_samples[i]
        end

	def advance_stream(s, i)
            prev_samples[i], next_samples[i] =
                next_samples[i], nil

	    header = s.advance
	    if header
                stream_time =
                    if use_rt then header.rt
                    else header.lg
                    end
                next_samples[i] = StreamSample.new stream_time, header.dup, s, i
            end
            current_samples[i] = prev_samples[i]
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
            min_sample = next_samples.compact.min { |s1, s2| s1.time <=> s2.time }
            if !min_sample
                return nil 
            end

            @current_pos += 1
            advance_stream(min_sample.stream, min_sample.stream_index)
	    @time = min_sample.time
            return min_sample.stream_index, min_sample.time,
                single_data(min_sample.stream_index)
        end

        # Decrements one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)

        def step_back
            max_sample = prev_samples.compact.max{ |s1, s2| s1.time <=> s2.time }
            if !max_sample
                return nil
            end

            @current_pos -= 1
            decrement_stream(max_sample.stream, max_sample.stream_index)
	    @time = max_sample.time
            return max_sample.stream_index, max_sample.time,
                single_data(max_sample.stream_index)
        end

        # Provided for backward compatibility only
        def count_samples; size end

        def size
            streams.map(&:size).inject(&:+)
        end

        # Returns the current data sample for the given stream index
        def single_data(index)
            s = current_samples[index]
            s.stream.data(s.header)
        end
    end
end

