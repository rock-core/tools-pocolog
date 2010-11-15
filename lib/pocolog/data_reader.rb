module Pocolog
    # Interface for reading a stream in a Pocolog::Logfiles
    class DataStream
        attr_reader :logfile
        attr_reader :index
        attr_reader :name
        attr_reader :type_name
        # Provided for backward compatibility reasons. Use #type_name
        def typename; type_name end

        attr_reader :marshalled_registry
        # The Logfiles::StreamInfo structure for that stream
        attr_reader :info
        # The index in the stream of the current sample
        attr_reader :sample_index

        def initialize(logfile, index, name, type_name, marshalled_registry)
            @logfile, @index, @name, @type_name, @marshalled_registry =
                logfile, index, name, type_name, marshalled_registry

            @registry = nil
            @sample_index = -1
        end

	# Returns a SampleEnumerator object for this stream
	def samples(read_data = true); SampleEnumerator.new(self, read_data) end

	# Enumerates the blocks of this stream
	def each_block(rewind = true)
            if rewind
                self.rewind
            end

            while advance
                yield if block_given?
            end
	end

	# Returns the +sample_index+ sample of this stream
	def [](sample_index)
	    samples.between(sample_index, nil).
		find { true }
	end

        attr_accessor :time_getter

        # Returns the time of the current sample
	def time
            header = logfile.data_header
            if !time_getter
                [header.rt, Time.at(header.lg - logfile.time_base)]
            else
                [header.rt, time_getter[data(header)]]
            end
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

        # True if we read past the last sample
        def eof?; size == sample_index end
        # True if the size of this stream is zero
        def empty?; size == 0 end

	# True if this data stream has a Typelib::Registry object associated
	def has_type?; !marshalled_registry.empty? end

	# Get the Typelib::Registry object for this stream
	def registry
	    if !@registry
		@registry = logfile.registry || Typelib::Registry.new

		stream_registry = Typelib::Registry.new

		if has_type?
                    Typelib::Registry.add_standard_cxx_types(stream_registry)
                    stream_registry.merge_xml(marshalled_registry)
                    stream_registry = stream_registry.minimal(typename)

                    # if we do have a registry, then adapt it to the local machine
                    # if needed. Right now, this is required if containers changed
                    # size.
                    resize_containers = Hash.new
                    stream_registry.each do |type|
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
            # The first sample in the file has index 0, so set sample_index to
            # -1 so that (@sample_index += 1) sets the index to 0 for the first
            # sample
            @sample_index = -1
            nil
	end

        # call-seq:
        #   first => [time_rt, time_lg, data]
        #
	# Returns the first sample in the stream, or nil if the stream is empty
        #
        # It differs from #rewind as it always decodes the data payload.
	def first
	    rewind
            self.next
        end

        # call-seq:
        #   last => [time_rt, time_lg, data]
        #
        # Returns the last sample in the stream, or nil if the stream is empty.
        def last
            last_sample_pos = info.interval_io[1]
            logfile.seek(last_sample_pos[1], last_sample_pos[0])
            @sample_index = size - 1
            self.next
        end

        # Seek the stream at the given position
        #
        # If +pos+ is a Time object, seeks to the last sample whose logical
        # time is not greater than +pos+
        #
        # If +pos+ is an integer, it is interpreted as an index and the stream
        # goes to the sample that has this index.
        #
        # Returns [rt, lg, data] for the current sample (if there is one), and
        # nil otherwise
	def seek(pos)
            @sample_index = logfile.seek_stream(self.index, pos)
            if header = self.data_header
                header = header.dup

                data = self.data(header)
                return [header.rt, Time.at(header.lg - logfile.time_base), data]
            end
	end

        # Reads the next sample in the file, and returns its header. Returns nil
        # if the end of file has been reached. Unlike +next+, it does not
        # decodes the data payload.
	def advance
            if sample_index < size
                @sample_index += 1
                logfile.each_data_block(index, sample_index == 0) do
                    return logfile.data_header
                end
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
            if sample_index < 0
                # Just rewind, never played
                return nil
            elsif sample_index == 0
                # Beginning of file reached
                rewind
                return nil 
            else
                seek(sample_index - 1)
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

