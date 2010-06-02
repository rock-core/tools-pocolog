module Pocosim
    DataStream = Struct.new :logfile, :index, :name, :typename, :marshalled_registry

    # Interface for reading a stream in a Pocosim::Logfiles
    class DataStream
        # The Logfiles::StreamInfo structure for that stream
        attr_reader :info
        # The index in the stream of the current sample
        attr_reader :sample_index

	# Returns a SampleEnumerator object for this stream
	def samples(read_data = true); SampleEnumerator.new(self, read_data) end

	# Enumerates the blocks of this stream
	def each_block(rewind = true)
            if rewind
                if !self.rewind
                    return
                end
                yield if block_given?
            end

	    logfile.each_data_block(index, false) do
                yield if block_given?
                @sample_index += 1
            end
	end

	# Returns the +sample_index+ sample of this stream
	def [](sample_index)
	    samples.between(sample_index, nil).
		find { true }
	end

        # Returns the time of the current sample
	def time
	    header = logfile.data_header
            [header.rt, Time.at(header.lg - logfile.time_base)]
	end

	# Get the logical time of first and last samples in this stream. If
	# +rt+ is true, returns the interval for the wall-clock time
        #
        # Returns nil if the stream is empty
	def time_interval(rt = false)
	    if rt then info.interval_rt
	    else info.interval_lg
	    end
	end

        # The data header for the current sample. You can store a copy of this
        # header to retrieve data later on with #data:
        #
        #   # Don't forget to duplicate !
        #   stored_header = stream.data_header.dup
        #   ...
        #   data = stream.data(stored_header)
	def data_header; logfile.data_header end

	# The size, in samples, of data in this stream
	def size; info.size end

	# True if this data stream has a Typelib::Registry object associated
	def has_type?; !marshalled_registry.empty? end

	# Get the Typelib::Registry object for this stream
	def registry
	    unless @registry
		@registry = logfile.registry || Typelib::Registry.new

		stream_registry = Typelib::Registry.new

		# Load the pocosim TLB in this registry, if it is found
		Pocosim.load_tlb(@registry)

		if has_type?
		    Tempfile.open('simlog_load_registry') do |io|
			io.write(marshalled_registry)
			io.flush
			
			stream_registry.import(io.path, 'tlb')
		    end
                    stream_registry = stream_registry.minimal(typename)

                    # if we do have a registry, then adapt it to the local machine
                    # if needed. Right now, this is required if containers changed
                    # size.
                    resize_containers = Hash.new
                    stream_registry.each_type do |type|
                        if type <= Typelib::ContainerType && type.size != type.natural_size
                            resize_containers[type] = type.natural_size
                        end
                    end
                    stream_registry.resize(resize_containers)

                    begin
                        @registry.merge(stream_registry)
                    rescue RuntimeError => e
                        if e.message =~ /but with a different definition/
                            raise e, e.message + ". Are you mixing 32 and 64 bit data ?", e.backtrace
                        end
                    end
		end
	    end
	    @registry
	end

	# Get a Typelib object describing the type of this data stream
	def type; @type ||= registry.get(typename) end

	# Returns the decoded data sample associated with the given block
        # header.
        #
        # Block headers are returned by #rewind 
	def data(data_header = nil)
	    data = type.wrap(logfile.data(data_header))
	    if logfile.endian_swap
		data.endian_swap
	    else
		data
	    end
	end

        # call-seq:
        #   rewind => data_header
        #
        # Goes to the first sample in the stream, and returns its header.
        # Returns nil if the stream is empty.
        #
        # It differs from #first as it does not decode the data payload.
	def rewind
            @sample_index = 0
	    logfile.each_data_block(index, true) do
                @sample_index += 1
		header = logfile.data_header
		next if header.lg == Time.at(0)
		return header
	    end
            nil
	end

        # call-seq:
        #   first => [time_rt, time_lg, data]
        #
	# Returns the first sample in the stream, or nil if the stream is empty
        #
        # It differs from #rewind as it always decodes the data payload.
	def first
	    header = rewind
            if header
                [header.rt, Time.at(header.lg - logfile.time_base), data]
            end
	end

        # call-seq:
        #   last => [time_rt, time_lg, data]
        #
        # Returns the last sample in the stream, or nil if the stream is empty.
        def last
            last_sample_pos = info.interval_io[1]
            logfile.seek(last_sample_pos[1], last_sample_pos[0])
            self.next
        end

        # Seek the stream at the first sample whose logical time is greater
        # than +pos+. Pos is either a Time -- in which case it is considered as
        # a logical time or an integer, in which case it is interpreted as an
        # index. Returns [rt, lg, data] for the sample just before (if there is
        # one)
	def seek(pos)
            @sample_index = logfile.seek_stream(self.index, pos)
            header = self.data_header.dup

	    if header
                data = self.data(header)
		return [header.rt, Time.at(header.lg - logfile.time_base), data]
	    end
	end

        # Reads the next sample in the file, and returns its header. Returns nil
        # if the end of file has been reached. Unlike +next+, it does not
        # decodes the data payload.
	def advance
	    logfile.each_data_block(index, false) do
                @sample_index += 1
		return logfile.data_header
	    end
	    nil
	end

	# call-seq:
        #   next => [time_rt, time_lg, data]
        #
        # Reads the next sample in the file, and returns it. It differs from
        # +advance+ as it always decodes the data sample.
	def next
	    header = advance
            if(header) 
              return [header.rt, Time.at(header.lg - logfile.time_base), data]
            end
	end

	# call-seq:
        #   previous => [time_rt, time_lg, data]
        #
        # Reads the previous sample in the file, and returns it.
        def previous
	    seek(sample_index - 1)
        end
    end

    class JointStream
	# Returns a SampleEnumerator object for this stream
	def samples(read_data = true); SampleEnumerator.new(self, read_data) end

	def name
	    streams.map { |s, _| s.name }.join(", ")
	end

	StreamSample = Struct.new :time, :header, :stream, :sample_index
	attr_reader :use_rt
	attr_reader :streams
	attr_reader :current_samples
	attr_reader :next_samples
	def initialize(use_rt = false, *streams)
	    @use_rt  = use_rt
	    @streams = []
	    streams.each_with_index do |s, i|
		@streams << [s, i]
	    end
            rewind
	end

	def time
	    max_sample = current_samples.max { |s1, s2| s1.time <=> s2.time }
	    [max_sample.time, max_sample.time]
	end

	DataHeader = Struct.new :rt, :lg, :headers
	def data_header
	    headers = current_samples.map { |s| s.header }

	    time = self.time[0]
	    DataHeader.new(time, time, headers)
	end

	def first
	    rewind 
	    [time.first, time.first, data]
	end
	def rewind; seek(nil) end

        # Advances the given stream, and returns the next sample in that stream.
	def advance_stream(s, i)
	    # Check that we haven't reached the end of the streams yet
	    return unless next_samples[i]
	    current_samples[i] = next_samples[i]

	    header = s.advance
	    if !header
		next_samples[i] = nil
		return
	    end

	    time = if use_rt then header.rt
		   else Time.at(header.lg - s.logfile.time_base)
		   end

	    next_samples[i] = StreamSample.new time, header, s, i
	end

	def seek(time_limit)
            @next_samples = @current_samples = nil
	    @next_samples = streams.map do |s, i|
		header = s.rewind
                return if !header

		time = if use_rt then header.rt
		       else Time.at(header.lg - s.logfile.time_base)
		       end

		StreamSample.new time, header, s, i
	    end

	    @current_samples = next_samples.map do |s|
		s.dup
	    end

	    time_limit ||= time.first

	    remaining = streams.find_all do |s, i|
		next_samples[i].time < time_limit
	    end

	    while !remaining.empty?
		remaining.delete_if do |s, i|
		    unless sample = advance_stream(s, i)
			return
		    end
		    sample.time >= time_limit
		end
	    end

	    header = data_header.dup
	    self.next
	    [header.rt, header.lg, data(header)]
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
	    return if !next_samples || !next_samples.all? { |s| s }
	    min_sample = next_samples.min { |s1, s2| s1.time <=> s2.time }
	    advance_stream(min_sample.stream, min_sample.sample_index)
	    return min_sample.sample_index, min_sample.time, single_data(min_sample.sample_index)
        end

        # Returns the current data sample for the given stream index
        def single_data(index)
            s = current_samples[index]
            s.stream.data(s.header)
        end

	def next
	    return unless next_samples.all? { |s| s }
	    min_sample = next_samples.min { |s1, s2| s1.time <=> s2.time }
	    advance_stream(min_sample.stream, min_sample.sample_index)
	    [min_sample.time, min_sample.time, data]
	end

        def previous
	    max_sample = current_samples.max { |s1, s2| s1.time <=> s2.time }
            stream_index = max_sample.sample_index

            if result = max_sample.stream.previous
              time = if use_rt then result[0]
                     else result[1]
                     end

              next_samples[stream_index] = current_samples[stream_index]
              current_samples[stream_index] =
                StreamSample.new(time, max_sample.stream.data_header, max_sample.stream, stream_index)
              [time, time, data]
            end
        end

	def each_block(rewind = true)
	    if rewind
		streams.each { |s, _| s.logfile.rewind }
		self.rewind
	    end

	    loop do
		if !self.next
		    return
		end
		yield
	    end
	end

	def data(header = nil)
	    if !header
		current_samples.map { |s| s.stream.data(s.header) }
	    else
		current_samples.enum_for(:each_with_index).map do |s, i|
		    s.stream.data(header.headers[i])
		end
	    end
	end
    end

    # Sample enumerators are nicer interfaces for data reading built on top of a DataStream
    # object
    class SampleEnumerator
	include Enumerable

	attr_accessor :use_rt
	attr_accessor :min_time,  :max_time,  :every_time
	attr_accessor :min_index, :max_index, :every_index
	attr_accessor :max_count
	def setvar(name, val)
	    time, index = case val
			  when Integer then [nil, val]
			  when Time then [val, nil] 
			  end

	    send("#{name}_time=", time)
	    send("#{name}_index=", index)
	end

	attr_reader :stream, :read_data
	def initialize(stream, read_data)
	    @stream = stream 
	    @read_data = read_data
	end
	def every(interval)
	    setvar('every', interval) 
	    self
	end
	def from(from);
	    setvar("min", from)
	    self
	end
	def to(to)
	    setvar("max", to)
	    self
	end
	def between(from, to)
	    self.from(from)
	    self.to(to)
	end
	def at(pos)
	    from(pos) 
	    max(1)
	end

	def realtime(use_rt = true)
	    @use_rt = use_rt 
	    self
	end
	def max(count)
	    @max_count = count
	    self
	end

	attr_accessor :next_sample
	attr_accessor :sample_count

	def each(&block)
	    self.sample_count = 0
	    self.next_sample = nil

	    last_data_block = nil

            if min_index || min_time
                stream.seek(min_index || min_time)
            end
	    stream.each_block(!(min_index || min_time)) do
                sample_index = stream.sample_index
		return self if max_index && max_index < sample_index
		return self if max_count && max_count <= sample_count

		rt, lg = stream.time
		sample_time = if use_rt then rt
			      else lg
			      end

		if min_time
		    if sample_time < min_time
			last_data_block = stream.data_header.dup
			next
		    elsif last_data_block
			last_data_time = if use_rt then last_data_block.rt
					 else last_data_block.lg
					 end
			yield_sample(last_data_time, sample_index - 1, last_data_block, &block)
			last_data_block = nil
		    end
		end
		return self if max_time && max_time < sample_time

		yield_sample(sample_time, sample_index, stream.data_header, &block)
	    end
	    self
	end

	# Yield the given sample if required by our configuration
	def yield_sample(sample_time, sample_index, data_block = nil)
	    do_display = !next_sample
	    if every_time 
		self.next_sample ||= sample_time
		while self.next_sample <= sample_time
		    do_display = true
		    self.next_sample += every_time.to_f
		end
	    elsif every_index
		self.next_sample ||= sample_index
		if self.next_sample <= sample_index
		    do_display = true
		    self.next_sample += every_index
		end
	    end

	    if do_display
		self.sample_count += 1
		yield(data_block.rt, data_block.lg, (stream.data(data_block) if read_data))
		last_data_block = nil
	    end
	end
    end
end

