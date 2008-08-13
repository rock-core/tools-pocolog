require 'utilrb/module/attr_predicate'
module Pocosim
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

	BlockInfo = Struct.new :pos, :type, :index, :payload_size
	attr_reader :block_info

	# Whether or not data bigger than COMPRESSION_MIN_SIZE should be
	# compressed using Zlib when written to this log file. Defaults to true
	attr_predicate :compress?

	attr_reader :io
	attr_reader :streams
	def initialize(*io)
	    @io          = io
	    @streams     = nil
	    @block_info  = BlockInfo.new
	    @compress    = true
	    rewind
	end

	def close
	    io.each { |file| file.close }
	end

	# The basename for creating new log files. The files
	# names are
	#
	#   #{basename}.#{index}.log
	attr_accessor :basename

	def tell; @next_block_pos end
	def seek(pos); @next_block_pos = pos end

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

	# Create an empty log file using +basename+ to build its name.
	# See #basename
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
	    @block_info  = BlockInfo.new
	    @time_base   = @time_base.dup
	    @time_offset = @time_offset.dup
	end

	# Start reading at the beginning of the first log file
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
	
	# Yields for each block found. The block header can be used
	# through #block_info
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

		unless header = io.read(BLOCK_HEADER_SIZE)
		    next_io
		    next
		end

		type, index, payload_size = header.unpack('CxvV')
		@block_info.pos          = @next_block_pos
		@block_info.type         = type
		@block_info.index        = index
		@block_info.payload_size = payload_size
		@next_block_pos = io.tell + payload_size

		if !BLOCK_TYPES.include?(type)
		    raise "invalid block type found #{type}, expected one of #{BLOCK_TYPES.join(", ")}"
		end

		yield(@block_info)
	    end
	rescue EOFError
	end

	# Yields for each data block in stream +stream_index+, or in all
	# streams if +stream_index+ is nil.
	def each_data_block(stream_index = nil, rewind = true, with_prologue = true)
	    each_block(rewind) do |block_info|
		if block_info.type == CONTROL_BLOCK
		    read_control_block
		elsif !stream_index || stream_index == block_info.index
		    if block_info.type == DATA_BLOCK
			if !declared_stream?(block_info.index)
			    raise "found data block for stream #{block_info.index} but this stream has never been declared"
			else
			    yield(block_info.index)
			end
		    elsif block_info.type == STREAM_BLOCK
			read_stream_declaration
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

	# The set of data streams found in this file. The file is read
	# the first time this function is called
	def streams
	    return @streams.compact if @streams

	    intervals = Hash.new
	    each_data_block(nil, true) do |stream_index|
		s = @streams[stream_index]
		info = (intervals[stream_index] ||= [nil, nil, 0])

		pos = [@rio, rio.tell]
		info[0] ||= pos
		info[1] =   pos
		info[2] += 1
	    end

            if !@streams
                return []
            end

	    @streams.each do |s|
		next unless s

		(first_io, first), (last_io, last), size = intervals[s.index]
		if size
		    rt_interval, lg_interval = [], []
		    @rio = first_io
		    rio.seek(first)
		    rt_interval[0], lg_interval[0] = [read_time, read_time]
		    @rio = last_io
		    rio.seek(last)
		    rt_interval[1], lg_interval[1] = [read_time, read_time]

		    s.instance_variable_set("@rt_interval", rt_interval)
		    s.instance_variable_set("@lg_interval", lg_interval)
		    s.instance_variable_set("@size", size || 0)
		    s.instance_variable_set("@read_info", true)
		else
		    s.instance_variable_set("@size", 0)
		end
	    end
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

	def read_stream_declaration # :nodoc:
	    if block_info.payload_size <= 8
		raise "bad data size #{payload_data.size}"
	    end

	    block_start   = rio.tell
	    type          = rio.read(1)
	    name_size     = rio.read(4).unpack('V').first
	    name          = rio.read(name_size)
	    typename_size = rio.read(4).unpack('V').first
	    typename      = rio.read(typename_size)

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
		@streams[stream_index] = DataStream.new(self.dup, stream_index, name, typename, registry || '')
	    end
	end

	# True if the host byte order is not the same than the file byte
	# order
	attr_reader :endian_swap

	# Reads a time in #rio and returns it 
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
	def data_header # :nodoc:
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

	# Writes a new block of the given type in the stream +index+.
	# +type+ is one of CONTROL_BLOCK, STREAM_BLOCK or DATA_BLOCK
	def write_block(type, index) # :nodoc:
	    payload = yield
	    wio << [type, index, payload.size].pack('CxvV')
	    wio << payload
	    
	    if wio.tell > MAX_FILE_SIZE
		new_file
	    end
	end

	def write_stream_declaration(index, name, type)
	    typename  = type.name
	    registry  = type.registry.to_xml
	    write_block(STREAM_BLOCK, index) do
		[DATA_STREAM, name.size, name, 
		    typename.size, typename,
		    registry.size, registry
		].pack("CVa#{name.size}Va#{typename.size}Va#{registry.size}")
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

	    write_block(DATA_BLOCK, stream.index) do
		[rt.tv_sec, rt.tv_usec, lg.tv_sec, lg.tv_usec,
		    data.length, compress, data
		].pack("#{DATA_BLOCK_HEADER_FORMAT}a#{data.size}")
	    end
	end
    end
end

