module Pocosim
    class StreamAligner
	StreamSample = Struct.new :time, :header, :stream, :stream_index

	attr_reader :use_rt

	attr_reader :streams
        attr_reader :current_streams
	attr_reader :current_samples
	attr_reader :next_samples

	def initialize(use_rt = false, *streams)
	    @use_rt  = use_rt
	    @streams = streams
            rewind
	end

        attr_reader :time

	def rewind
            @current_samples = Array.new
            @next_samples    = Array.new
            @current_streams = Array.new

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

	def advance_stream(s, i)
            if !next_samples[i]
                current_samples.delete_at(i)
                current_streams.delete_at(i)
                next_samples.delete_at(i)
                current_samples.each do |s|
                    if s.index > i
                        s.index -= 1
                    end
                end
                next_samples.each do |s|
                    if s.index > i
                        s.index -= 1
                    end
                end
                return
            end

	    current_samples[i] = next_samples[i]

	    header = s.advance
	    if !header
		next_samples[i] = nil
		return current_samples[i]
	    end

	    time = current_streams[i].time
            @time = if use_rt then time.first
                    else time.last
                    end

	    next_samples[i] = StreamSample.new self.time, header.dup, s, i
            current_samples[i]
	end

        # call-seq:
        #   joint_stream.step => updated_stream_index, time
        #
        # Advances one step in the joint stream, an returns the index of the
        # updated stream as well as the time.
        #
        # The associated data sample can then be retrieved by
        # single_data(stream_idx)
        def step
	    min_sample = next_samples.compact.min { |s1, s2| s1.time <=> s2.time }
	    advance_stream(min_sample.stream, min_sample.stream_index)
	    return min_sample.stream_index, min_sample.time, single_data(min_sample.stream_index)
        end

        # Returns the current data sample for the given stream index
        def single_data(index)
            s = current_samples[index]
            s.stream.data(s.header)
        end
    end
end

