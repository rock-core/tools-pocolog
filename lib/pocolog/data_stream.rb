module Pocolog
    # Interface for reading a stream in a Pocolog::Logfiles
    class DataStream
        attr_reader :logfile
        attr_reader :index
        attr_reader :name

        attr_reader :type

        # The {StreamInfo} structure for that stream
        attr_reader :info
        # The index in the stream of the last read sample
        #
        # It is equal to size if we are past-the-end, i.e. if one called #next
        # until it returned nil
        attr_reader :sample_index
        # The stream associated metadata
        attr_reader :metadata

        def initialize(logfile, index, name, type, metadata = Hash.new, info = StreamInfo.new)
            @logfile, @index, @name, @metadata, @info =
                logfile, index, name, metadata, info

            # if we do have a registry, then adapt it to the local machine
            # if needed. Right now, this is required if containers changed
            # size.
            registry = type.registry
            resize_containers = Hash.new
            registry.each do |type|
                if type <= Typelib::ContainerType && type.size != type.natural_size
                    resize_containers[type] = type.natural_size
                end
            end
            if resize_containers.empty?
                @type = type
            else
                registry.resize(resize_containers)
                @type = registry.get(type.name)
            end
	    
            @data = nil
            @sample_index = -1
        end

        def stream_index
            info.index
        end

        def closed?
            logfile.closed?
        end

        def open
            logfile.open
        end

        def close
            logfile.close
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
                [header.rt, header.lg]
            else
                [header.rt, time_getter[data(header)]]
            end
	end

	# Get the logical time of first and last samples in this stream. If
	# +rt+ is true, returns the interval for the wall-clock time
        #
        # Returns nil if the stream is empty
	def time_interval(rt = false)
	    if rt
                info.interval_rt.map { |t| StreamIndex.time_from_internal(t, 0) }
            else
                info.interval_lg.map { |t| StreamIndex.time_from_internal(t, 0) }
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

        # Reload the registry. Can be useful if new convertions have been added
        # to the Typelib system
        def reload_registry
            @registry = nil
            @type = nil
            registry
        end

	# Get the Typelib::Registry object for this stream
	def registry
            type.registry
	end

	#Returns the decoded subfield specified by 'fieldname'
	#for the given data header. If no header is given, the
	#current last read data header is used
	def sub_field(fieldname, data_header = nil)
	    header = data_header || logfile.data_header
	    if( header.compressed )
		data(data_header).send(fieldname)
	    elsif(type.is_a?(Typelib::CompoundType) and type.has_field?(fieldname))
		offset = type.offset_of(fieldname)
		subtype = type[fieldname]
		rawData = logfile.sub_field(offset, subtype.size, data_header)
		wrappedType = subtype.wrap(rawData)
		rubyType = Typelib.to_ruby(wrappedType)
		rubyType
	    else
		nil
	    end
	end
	    
	# Returns the decoded data sample associated with the given block
        # header.
        #
        # Block headers are returned by #rewind 
	def raw_data(data_header = nil, sample = nil)
	    if(@data && !data_header) then @data
	    else
                marshalled_data = logfile.data(data_header)
		data = sample || type.new
                data.from_buffer_direct(marshalled_data)
		if logfile.endian_swap
		    data = data.endian_swap
		end
                data
	    end
        rescue Interrupt
            raise
        rescue Exception => e
            raise e, "failed to unmarshal sample at #{(data_header || logfile.data_header).payload_pos}: #{e.message}", e.backtrace
	end

        def read_one_data_sample(position)
            Typelib.to_ruby(read_one_raw_data_sample(position))
        end

        def read_one_raw_data_sample(position, sample = nil)
	    block_pos = stream_index.file_position_by_sample_number(position)
            marshalled_data = logfile.read_one_data_payload(block_pos)
            data = sample || type.new
            data.from_buffer_direct(marshalled_data)
            if logfile.endian_swap
                data = data.endian_swap
            end
            data
        rescue Interrupt
            raise
        rescue Exception => e
            raise e, "failed to unmarshal sample for block position #{block_pos}: #{e.message}", e.backtrace
        end

        def data(data_header = nil)
            Typelib.to_ruby(raw_data(data_header))
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
        #
        # After a call to #first, #sample_index is 0
	def first
	    rewind
            self.next
        end

        # call-seq:
        #   last => [time_rt, time_lg, data]
        #
        # Returns the last sample in the stream, or nil if the stream is empty.
        #
        # After a call to #last, #sample_index is size - 1
        def last
            @sample_index = size - 2
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
	def seek(pos, decode_data = true)
	    if pos.kind_of?(Time)
                return nil if(time_interval.empty? || time_interval[0] > pos || time_interval[1] < pos)
		@sample_index = stream_index.sample_number_by_time(pos)
	    else
		@sample_index = pos
	    end

	    file_pos = stream_index.file_position_by_sample_number(@sample_index)
	    block_info = logfile.read_one_block(file_pos)
            if block_info.stream_index != self.index
                raise InternalError, "index returned index=#{@sample_index} and pos=#{file_pos} as position for seek(#{pos}) but it seems to be a sample in stream #{logfile.stream_from_index(block_info.stream_index).name} while we were expecting #{name}"
            end

            if header = self.data_header
                header = header.dup
		if decode_data
		    data = self.data(header)
		    return [header.rt, header.lg, data]
		else
		    header
		end
            end
	end

        # Reads the next sample in the file, and returns its header. Returns nil
        # if the end of file has been reached. Unlike +next+, it does not
        # decodes the data payload.
	def advance
            if sample_index < size-1
                @sample_index += 1
		file_pos = stream_index.file_position_by_sample_number(@sample_index)
		logfile.read_one_block(file_pos)
		return logfile.data_header
            else
                @sample_index = size
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
            if header
                return [header.rt, header.lg, data]
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

	# call-seq:
        #   copy_to(index1,index2,stream) => true 
        #   copy_to(time1,time2,stream) => true 
        #
        # copies all blocks from start_index/time to end_index/time to the given stream
        # for each block the given code block is called. If the code block returns 1
        # the copy process will be canceled and the method returns false 
        #
        # The given interval is automatically truncated if it is too big
        def copy_to(start_index = 0, end_index = size, stream, &block)
            interval = time_interval
            return unless interval.first
            start_index = if start_index.is_a? Time
                              if interval.first > start_index
                                  0
                              else
                                  stream_index.sample_number_by_time(start_index)
                              end
                          else
                              if start_index < 0
                                  0
                              else
                                  start_index
                              end
                          end
            end_index = if end_index.is_a? Time
                            if interval.last < end_index
                                size
                            else
                                stream_index.sample_number_by_time(end_index)
                            end
                        else
                            if end_index >= size
                                size
                            else
                                end_index
                            end
                        end
            
            counter = 0
            data_header = seek(start_index, false)
            while sample_index < end_index
                if block
                    return false if block.call(counter)
                end
                data_buffer = logfile.data(data_header)
                stream.write_raw(data_header.rt_time, data_header.lg_time, data_buffer)
                counter += 1
                data_header = advance
            end
            counter
        end

	# call-seq:
        #   samples?(pos1,pos2) => true 
        #   samples?(time1,time2) => true 
        #
        # returns true if stream samples lies insight the given time or position interval
        def samples?(start_index,end_index)
            if start_index.is_a? Time
                interval = time_interval
                return unless interval.first
                start_index <= interval.last && start_index <= end_index && end_index >= interval.first
            else
                start_index < size && start_index <= end_index && end_index >= 0
            end
        end

	# Write a sample in this stream, with the +rt+ and +lg+
	# timestamps. +data+ can be either a Typelib::Type object of the
	# right type, or a String (in which case we consider that it is
	# the raw data)
	def write(rt, lg, data)
            data = Typelib.from_ruby(data, type)
            write_raw(rt, lg, data.to_byte_array)
	end

        # Write an already marshalled sample. +data+ is supposed to be a
        # typelib-marshalled value of the stream type
        def write_raw(rt, lg, data)
	    logfile.write_data_block(self, rt, lg, data)
        end
    end
end

