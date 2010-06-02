require 'utilrb/module/attr_predicate'
require 'fileutils'
module Pocosim
    class InvalidIndex < RuntimeError; end
    class Logfiles
	FORMAT_VERSION = 2

	BLOCK_HEADER_SIZE = 8
	TIME_SIZE = 8
	DATA_HEADER_SIZE = TIME_SIZE * 2 + 5

	# Data blocks of less than COMPRESSION_MIN_SIZE are never compressed
	COMPRESSION_MIN_SIZE = 500
	# If the size gained by compressing is below this value, do not save in
	# compressed form
	COMPRESSION_THRESHOLD = 0.3

	class ObsoleteVersion < RuntimeError; end
	class MissingPrologue < RuntimeError; end

	BlockInfo = Struct.new :io, :pos, :type, :index, :payload_size
	attr_reader :block_info

	# Whether or not data bigger than COMPRESSION_MIN_SIZE should be
	# compressed using Zlib when written to this log file. Defaults to true
	attr_predicate :compress?

        def self.valid_file?(file)
            Logfiles.new(file)
            true
        rescue
            false
        end

	attr_reader :io
	attr_reader :streams
	attr_reader :registry
	def initialize(*io)
	    if io.last.kind_of?(Typelib::Registry)
		@registry = io.pop
	    end

	    @io          = io
            @io_size     = io.map { |rio| rio.stat.size }
	    @streams     = nil
	    @block_info  = BlockInfo.new
	    @compress    = true
	    rewind
            read_prologue
	end

	def close
	    io.each { |file| file.close }
	end

	# The basename for creating new log files. The files
	# names are
	#
	#   #{basename}.#{index}.log
	attr_accessor :basename

        # Returns the current position in the file
        #
        # This is the position of the next block.
	def tell; @next_block_pos end

        # call-seq:
        #   seek(data_header) => position
        #   seek(raw_position[, rio]) => position
        #
        # Seeks in the file at the given position. In the first form, seeks to
        # the place where the data_header is stored. In the second form, seeks
        # to the given raw position, and optionally changes the current IO
        # object (rio is an index in the set of IOs given to #initialize)
	def seek(pos, rio = nil)
            if pos.kind_of?(DataHeader)
                unless io_index = @io.index(pos.io)
                    raise "#{pos} does not come from this log fileset"
                end
                @next_block_pos = pos.pos
            else
                if rio
                    @rio = rio
                end
                @next_block_pos = pos
            end
        end

	# A new log file is created when the current one has reached this
	# size in bytes
	MAX_FILE_SIZE = 100 * 1024**2

	# Continue writing logs in a new file. See #basename to know how
	# files are named
	def new_file
	    name = "#{basename}.#{@io.size}.log"
	    io = File.new(name, 'w')
	    Logfiles.write_prologue(io)
	    @io << io
	    streams.each_with_index do |s, i|
		write_stream_declaration(i, s.name, s.type)
	    end
	end

        # Opens a set of file. +pattern+ can be a globbing pattern, in which
        # case all the matching files will be opened as a log sequence
        def self.open(pattern, registry = nil)
            io = Dir.enum_for(:glob, pattern).map { |name| puts name ; File.open(name) }
            if io.empty?
                raise ArgumentError, "no files matching '#{pattern}'"
            end

            if registry
                io << registry
            end
            new(*io)
        end

	# Create an empty log file using +basename+ to build its name.
        # Namely, it will create a new file named <basename>.0.log. Then,
        # calls to #new_file would create <basename>.1.log and so on
	def self.create(basename)
	    file = Logfiles.new
	    file.basename = basename
	    file.instance_variable_set("@streams", Array.new)
	    file.new_file

	    file
	end

	# Open an already existing set of log files or create it
	def self.append(basename)
	    io = []
	    i = 0
	    while File.readable?(path = "#{basename}.#{i}.log")
		io << File.open(path, 'a+')
		i += 1
	    end

	    if io.empty?
		return create(basename)
	    end

	    file = Logfiles.new(*io)
	    file.basename = basename
	    file
	end

	def initialize_copy(from) # :nodoc:
	    super

	    @io		 = from.io.map { |obj| obj.dup }
	    @registry    = from.registry
	    @block_info  = BlockInfo.new
	    @time_base   = @time_base.dup
	    @time_offset = @time_offset.dup
	end

	# Returns at the beginning of the first file of the file set
	def rewind
	    @rio     = 0
	    @time_base      = []
	    @time_offset    = []
	    @next_block_pos = 0

	    @data_header = DataHeader.new
	    @data = nil
	end

	attr_reader :format_version # :nodoc:
	MAGIC = "POCOSIM" # :nodoc:
	    
	def read_prologue # :nodoc:
	    io = rio
	    io.seek(0)
	    magic	   = io.read(MAGIC.size)
	    if magic != MAGIC
		# Not a valid file. Make the user try --export
		raise MissingPrologue, "invalid prologue in #{io.path}. Try the --to-new-format of pocosim-log if it is an old file"
	    end

	    @format_version, big_endian = io.read(9).unpack('xVV')
	    @endian_swap = ((big_endian != 0) ^ Pocosim.big_endian?)
	    if format_version < FORMAT_VERSION
		raise ObsoleteVersion, "old format #{format_version}, current format is #{FORMAT_VERSION}. Convert it using the --to-new-format of pocosim-log"
	    elsif format_version > FORMAT_VERSION
		raise "this file is in v#{format_version} which is newer that the one we know #{FORMAT_VERSION}. Update pocosim"
	    end
	    @next_block_pos = rio.tell
	end

	# Continue reading on the next IO object, or raise EOFError if we
	# are currently reading the last one
	def next_io
	    @rio += 1
	    if @io.size == @rio
		raise EOFError
	    else
		read_prologue
		rio
	    end
	end

	# Returns the IO object used for reading
	def rio; @io[@rio] end
	# Returns the IO object used for writing
	def wio; @io.last end
        # Returns the file size of +rio+
        def file_size; @io_size[@rio] end
	
        # Yields for each block found in the file set.
        #
        # This is not meant for direct use. Use #each_data_block instead.
	def each_block(rewind = true, with_prologue = true)
	    self.rewind if rewind
	    while true
		io = self.rio
		if @next_block_pos == 0 && with_prologue
		    read_prologue
		else
		    io.seek(@next_block_pos)
		end

		@data = nil
		@data_header.updated = false

                if !read_block_header
                    next_io
                    next
                end

		yield(@block_info)
	    end
	rescue EOFError
	end

        # Finds the entry in the index just before the position specified by
        # +pos+ in the given stream.
        #
        # +stream_idx+ is the stream index. +pos+ is either an integer or a
        # Time.
        #
        # If +pos+ is an integer, it is interpreted as a sample index in the
        # stream. #seek_stream will then return the sample index of the entry
        # found in the index.
        #
        # If +pos+ is a time, it is interpreted as a sample time. #seek_stream
        # will then return the sample time of the entry found in the index.
        #
        # Raises ArgumentError if no sample can be found that matches +pos+
        def seek_stream(stream_idx, pos)
            info = streams[stream_idx].info
            if info.empty?
                raise ArgumentError, "#{pos} out of bounds"
            end

            if pos.kind_of?(Integer)
                index_entry = info.index.find do |size, _|
                    size + Logfiles::StreamInfo::INDEX_STEP > pos
                end

                unless index_entry = info.index.find { |size, _| size + Logfiles::StreamInfo::INDEX_STEP > pos }
                    raise ArgumentError, "cannot find #{pos} in index"
                end

                header = nil
                sample_index = index_entry[0]
                seek(index_entry[1][1], index_entry[1][0])
                each_data_block(stream_idx, false) do
                    break if sample_index == pos
                    sample_index += 1
                end

                if sample_index != pos
                    raise InternalError, "inconsistency in #seek: seek(#{pos}) led to sample_index == #{sample_index}"
                end

                return sample_index

            elsif pos.kind_of?(Time)
                if pos < info.interval_lg[0] || pos > info.interval_lg[1]
                    raise ArgumentError, "#{pos} is out of bounds"
                end
                index_entry = info.index.find_index { |_, _, _, lg| lg > pos }
                if index_entry
                    index_entry = info.index[index_entry - 1]
                else
                    index_entry = info.index.last
                end

                header = nil
                @sample_index = index_entry[0]
                seek(index_entry[1][1], index_entry[1][0])
                each_data_block(stream_idx, false) do
                    break if data_header.lg > pos
                end

                return data_header.lg
            end
        end

        # Reads one block at the specified position and returns the block type
        # (equal to block_info.type). If the block is a control or stream block,
        # also call the relevant parsing methods.
        #
        # See #seek for the meaning of +pos+ and +rio+. If both are nil, reads
        # the sample at the current position.
        def read_one_block(pos = nil, rio = nil)
            if pos
                seek(pos, rio)
            end
            each_block(false) do |block_info|
                return handle_block(block_info)
            end
            nil
        end

        # Gets the block information in +block_info+ and acts accordingly: calls
        # the relevant parsing methods if it is a control or stream block. It
        # does nothing for data blocks.
        #
        # Returns the block type (CONTROL_BLOCK, DATA_BLOCK or STREAM_BLOCK)
        def handle_block(block_info) # :nodoc:
            if block_info.type == CONTROL_BLOCK
                read_control_block
            elsif block_info.type == DATA_BLOCK
                if !declared_stream?(block_info.index)
                    raise "found data block for stream #{block_info.index} but this stream has never been declared"
                end
            elsif block_info.type == STREAM_BLOCK
                read_stream_declaration
            end
            block_info.type
        end

        # Reads the block header at the current position. Returns true if the
        # read was successful and false if we reached the end of file.
        #
        # It updates the @block_info and @next_block_pos instance variable
        # accordingly.
        def read_block_header # :nodoc:
            unless header = rio.read(BLOCK_HEADER_SIZE)
                return
            end

            type, index, payload_size = header.unpack('CxvV')
            next_block_pos = rio.tell + payload_size
            if file_size < next_block_pos
                return
            end

            @block_info.io           = @rio
            @block_info.pos          = @next_block_pos
            @block_info.type         = type
            @block_info.index        = index
            @block_info.payload_size = payload_size
            @next_block_pos = next_block_pos

            if !BLOCK_TYPES.include?(type)
                raise "invalid block type found #{type}, expected one of #{BLOCK_TYPES.join(", ")}"
            end
            true
        end

	# call-seq:
        #   each_data_block([stream_index[, rewind]]) do |stream_index|
        #   end
        #
        # Yields for each data block in stream +stream_index+, or in all
	# streams if +stream_index+ is nil. The block header can be retrieved
        # using #data_header, and the sample by using #data
        #--
        # the with_prologue parameter is a backward compatibility feature that
        # allowed to read old files that did not have a prologue.
	def each_data_block(stream_index = nil, rewind = true, with_prologue = true)
	    each_block(rewind) do |block_info|
                if handle_block(block_info) == DATA_BLOCK
                    if !stream_index || stream_index == block_info.index
                        yield(block_info.index)
                    end
                end
	    end

	rescue EOFError
	rescue
	    if !rio
		raise $!
	    else
		raise $!, "#{$!.message} at position #{rio.pos}", $!.backtrace
	    end
	end

        # Basic information about a stream, as saved in the index files
        class StreamInfo
            INDEX_STEP = 500

            # Position of the declaration block as [raw_pos, io_index]. This
            # information can directly be given to Logfiles#seek
            attr_accessor :declaration_block
            # The position of the first and last samples in the file set, as
            # [[raw_pos, io_index], [raw_pos, io_index]]. It is empty for empty
            # streams.
            attr_accessor :interval_io
            # The logical time of the first and last samples of that stream
            # [beginning, end]. It is empty for empty streams.
            attr_accessor :interval_lg
            # The real time of the first and last samples of that stream
            # [beginning, end]. It is empty for empty streams.
            attr_accessor :interval_rt
            # The number of samples in this stream
            attr_accessor :size
            # The index data itself. It is an ordered set of 4-tuples:
            #     [sample_index, [raw_pos, io_index], real_time, logical_time]
            #
            # Seeking using this index is done by Logfiles#seek_stream
            attr_accessor :index

            # True if this stream is empty
            def empty?; size == 0 end

            def initialize
                @interval_io = []
                @interval_lg = []
                @interval_rt = []
                @size        = 0
                @index       = []
            end
        end

        # Load the given index file. Returns nil if the index file does not
        # match the files in the file set.
        def load_index_file(index_filename)
            # Look for an index. If it is found, load it and use it.
            return unless File.readable?(index_filename)
            STDERR.print "loading file info from #{index_filename}... "
            file_info, stream_info = Marshal.load(File.open(index_filename))

            if file_info.size != @io.size
                raise InvalidIndex, "invalid index file: file set changed"
            end
            coherent = file_info.enum_for(:each_with_index).all? do |(size, time), idx|
                size == File.size(@io[idx].path)
            end
            if !coherent
                raise InvalidIndex, "invalid index file: file size is different"
            end

            stream_info.each_with_index do |info, idx|
                if !info.declaration_block
                    raise InvalidIndex, "old index file found"
                end

                @rio, pos = info.declaration_block
                if read_one_block(pos, @rio) != STREAM_BLOCK
                    raise InvalidIndex, "invalid declaration_block reference in index"
                end

                # Read the stream declaration block and then update the
                # info attribute of the stream object
                if !info.empty?
                    @rio, pos = info.interval_io[0]
                    if read_one_block(pos, @rio) != DATA_BLOCK
                        raise InvalidIndex, "invalid start IO reference in index"
                    end

                    if block_info.index != idx
                        raise InvalidIndex, "invalid interval_io: stream index mismatch for #{@streams[idx].name}. Expected #{idx}, got #{data_block_index}."
                    end
                    @streams[idx].instance_variable_set(:@info, info)
                end
            end
            STDERR.puts "done"
            return @streams.compact

        rescue InvalidIndex => e
            STDERR.puts "invalid index file"
        end

	# Loads and returns the set of data streams found in this file. Will
        # lazily build an index file when required.
	def streams
	    return @streams.compact if @streams

            index_filename = File.basename(@io[0].path, File.extname(@io[0].path)) + ".idx"
            index_filename = File.join(File.dirname(@io[0].path), index_filename)
            if streams = load_index_file(index_filename)
                return streams
            end

            # No index file. Compute it.
            STDERR.print "building index ..."
	    each_data_block(nil, true) do |stream_index|
                # The stream object itself is built when the declaration block
                # has been found
                s    = @streams[stream_index]
                info = s.info
                info.interval_io[1] = [@rio, block_info.pos]
                info.interval_io[0] ||= info.interval_io[1]

                if info.size % StreamInfo::INDEX_STEP == 0
                    info.index << [info.size, info.interval_io[1].dup, read_time, read_time]
                end
                info.size += 1
	    end

            if !@streams
                STDERR.puts "done"
                return []
            end

	    @streams.each do |s|
		next unless s

                stream_info = s.info
		if !stream_info.empty?
		    @rio, pos = stream_info.interval_io[0]
		    rio.seek(pos + BLOCK_HEADER_SIZE)
                    stream_info.interval_rt[0] = read_time
                    stream_info.interval_lg[0] = read_time
		    @rio, pos = stream_info.interval_io[1] || stream_info.interval_io[0]
		    rio.seek(pos + BLOCK_HEADER_SIZE)
                    stream_info.interval_rt[1] = read_time
                    stream_info.interval_lg[1] = read_time
		end
	    end

            file_info   = @io.map { |io| [File.size(io.path), io.mtime] }
            stream_info = @streams.compact.map { |s| s.info }

            begin
                File.open(index_filename, 'w') do |io|
                    Marshal.dump([file_info, stream_info], io)
                end
            rescue
                FileUtils.rm_f index_filename
                raise
            end
            STDERR.puts "done"
	    @streams.compact
	end

	# True if there is a stream +index+
	def declared_stream?(index)
	    @streams && (@streams.size > index && @streams[index]) 
	end

	# Returns the Time object which describes the 'zero' of this data
	# set
	def time_base
	    if @time_base.empty?
		Time.at(0)
	    else
		@time_base.last[1] 
	    end
	end

	# Returns the offset from #time_base
	def time_offset
	    if @time_offset.empty? then 0
	    else @time_offset.last[1] 
	    end
	end

	# Reads a control block, which is used to set either #time_base
	# or #time_offset
	def read_control_block # :nodoc:
	    control_time  = read_time
	    control_block_type = rio.read(1).unpack('C').first
	    control_value = read_time
	    if control_block_type == CONTROL_SET_TIMEBASE
		@time_base << [control_time, control_value]
	    elsif control_block_type == CONTROL_SET_TIMEOFFSET
		@time_offset << [control_time, Float(control_value.tv_sec) + control_value.tv_usec / 1.0e6]
	    else 
		raise "unknown control block type #{control_block_type}"
	    end
	end

        # Reads the stream declaration block present at the current position in
        # the file.
        #
        # Raise if a new definition block is found for an already existing
        # stream, and the definition does not match the old one.
	def read_stream_declaration # :nodoc:
	    if block_info.payload_size <= 8
		raise "bad data size #{block_info.size}"
	    end

            io_index      = @rio
	    block_start   = rio.tell
	    type          = rio.read(1)
	    name_size     = rio.read(4).unpack('V').first
	    name          = rio.read(name_size)
	    typename_size = rio.read(4).unpack('V').first
	    typename      = rio.read(typename_size)

            # Load the registry if it seems that there is one
	    unless rio.tell == block_start + block_info.payload_size
		registry_size = rio.read(4).unpack('V').first
		registry      = rio.read(registry_size)
	    end

	    stream_index = block_info.index
	    if @streams && (old = @streams[stream_index])
		unless old.name == name && old.typename == typename || old.registry == registry
		    raise "stream #{name} changed definition"
		end
		old
	    else
		@streams ||= Array.new
		s = (@streams[stream_index] = DataStream.new(self.dup, stream_index, name, typename, registry || ''))

                info = StreamInfo.new
                s.instance_variable_set(:@info, info)
                info.declaration_block = [io_index, block_info.pos]
                s
	    end
	end

	# True if the host byte order is not the same than the file byte
	# order
	attr_reader :endian_swap

        # Reads a time at the current position, and returns it as a Time object
	def read_time # :nodoc:
	    rt_sec, rt_usec = rio.read(TIME_SIZE).unpack('VV')
	    Time.at(rt_sec, rt_usec)
	end

	DataHeader = Struct.new :io, :pos, :rt, :lg, :size, :compressed, :updated

	# Reads the header of a data block. This sets the @data_header
	# instance variable to a new DataHeader object describing the
	# current block. If you want to keep a reference on a data block,
	# and read it later, do the following
	#
	#   block = file.data_header.dup
	#   [do something, including reading the file]
	#   data  = file.data(block)
	def data_header
	    if @data_header.updated
		@data_header
	    else
		data_block_pos = rio.tell
		rt, lg = read_time, read_time
		data_size, compressed = rio.read(5).unpack('VC')

		size = rio.tell + data_size - data_block_pos
		expected = block_info.payload_size
		if size != expected
		    raise "payload was supposed to be #{expected} bytes, but found #{size}"
		end

		@data_header.io  = rio
		@data_header.pos = rio.tell
		@data_header.rt = rt
		@data_header.lg = lg
		@data_header.size = data_size
		@data_header.compressed = (compressed != 0)
		@data_header.updated = true
		@data_header
	    end
	end

	# Returns the raw data payload of the current block
	def data(data_header = nil)
	    if @data then @data
	    else
		data_header ||= self.data_header
		data_header.io.seek(data_header.pos)
		data = data_header.io.read(data_header.size)
		if data_header.compressed
		    # Payload is compressed
		    data = Zlib::Inflate.inflate(data)
		end
		@data = data
	    end
	end

        # Formats a block and writes it to +io+
        def self.write_block(wio, type, index, payload)
	    wio << [type, index, payload.size].pack('CxvV')
	    wio << payload
            return wio
        end

        def self.write_stream_declaration(wio, index, name, type, type_registry = nil)
            if !type_registry
                if type.kind_of?(Typelib::Type)
                    type_registry = type.registry.to_xml
                    type_name = type.name
                else
                    raise ArgumentError, "expected either a Type class or a type name, XML type registry pair"
                end
            else
                type_name = type.to_str
            end

            payload = [DATA_STREAM, name.size, name, 
                type_name.size, type_name,
                type_registry.size, type_registry
            ].pack("CVa#{name.size}Va#{type_name.size}Va#{type_registry.size}")
	    write_block(wio, STREAM_BLOCK, index, payload)
        end

	def do_write # :nodoc:
            yield
	    
	    if wio.tell > MAX_FILE_SIZE
		new_file
	    end
	end

	def write_stream_declaration(index, name, type)
            do_write do
                Logfiles.write_stream_declaration(wio, index, name, type)
            end
	end

	# Returns the DataStream object for +name+, +registry+ and
	# +type+. Optionally creates it.
	def stream(name, type = nil, create = false)
	    if s = streams.find { |s| s.name == name }
		return s
	    elsif !type || !create
		raise ArgumentError, "no such stream #{name}"
	    end

	    @streams ||= Array.new
	    new_index = @streams.size
	    write_stream_declaration(new_index, name, type)

	    typename  = type.name
	    registry  = type.registry.to_xml
	    stream = DataStream.new(self, new_index, name, typename, registry)
	    @streams << stream
	    stream
	end

	# Creates a JointStream object on the streams whose names are given.
	# The returned object is used to coherently iterate on the samples of
	# the given streams (i.e. it will yield samples that are valid at the
	# same time)
	def joint_stream(use_rt, *names)
	    streams = names.map do |n|
		stream(n)
	    end
	    JointStream.new(use_rt, *streams)
	end

	TIME_PADDING = TIME_SIZE - 8
	DATA_BLOCK_HEADER_FORMAT = "VVx#{TIME_PADDING}VVx#{TIME_PADDING}VC"

	def write_data_block(stream, rt, lg, data) # :nodoc:
	    compress = 0
	    if compress? && data.size > COMPRESSION_MIN_SIZE
		data = Zlib::Deflate.deflate(data)
		compress = 1
	    end

            do_write do
		payload = [rt.tv_sec, rt.tv_usec, lg.tv_sec, lg.tv_usec,
		    data.length, compress, data
		].pack("#{DATA_BLOCK_HEADER_FORMAT}a#{data.size}")
                write_block(DATA_BLOCK, stream.index, payload)
            end
	end
    end
end

