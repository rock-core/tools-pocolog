require 'utilrb/module/attr_predicate'
require 'yaml'
require 'fileutils'
module Pocolog
    class InvalidIndex < RuntimeError; end
    class InternalError < RuntimeError; end

    # Low-level access to (consistent) set of logfiles.
    #
    # The pocolog logfiles can be split during recording in order to limit each
    # file's size. This class allows to provide a list of files (to
    # Logfiles.open) and have a uniform access to the data.
    #
    # Files are indexed (i.e. a .idx file gets generated along with the log
    # file) to provide quick random access.
    #
    # A higher level access is provided by the streams (DataStream), that can be
    # retrieved with #stream, #stream_from_type and #stream_from_index
    #
    # == Format
    #
    # Pocolog files are made of:
    # 
    # * a prologue
    # * a sequence of generic blocks, each block pointing to the next block
    # * blocks can either be stream blocks, control blocks or data blocks.
    #   * Stream blocks define a new data stream with name, type name and type
    #     definition, assigning it a stream ID which is unique in these logfiles
    #   * Control blocks provide additional, logfile-wide, information. They are
    #     not assigned to streams. This feature is currently unused.
    #   * Data blocks store a single data sample in a stream
    #
    # See the tasks/logging.hh file in Rock's tools/logger package for a
    # detailed description of each block's layout.
    class Logfiles
        # Format version ID. Increment this when the file format changes in a
        # non-backward-compatible way
	FORMAT_VERSION = 2

        # The size of the generic block header
	BLOCK_HEADER_SIZE = 8
        # The size of a time in a block header
	TIME_SIZE = 8
        # The size of a data header, excluding the generic block header
	DATA_HEADER_SIZE = TIME_SIZE * 2 + 5

	# Data blocks of less than COMPRESSION_MIN_SIZE are never compressed
	COMPRESSION_MIN_SIZE = 500
	# If the size gained by compressing is below this value, do not save in
	# compressed form
	COMPRESSION_THRESHOLD = 0.3

        # Exception thrown when opening if the log file is not 
        #
        # One should run pocolog --upgrade-version when this happen
	class ObsoleteVersion < RuntimeError; end
        # Logfiles.open could not find a valid prologue in the provided file(s)
        #
        # This is most often because the provided file(s) are not pocolog files
	class MissingPrologue < RuntimeError; end

        # Structure that stores additional information about a block
	BlockInfo = Struct.new :io, :pos, :type, :index, :payload_size
        # The BlockInfo instance storing information about the last block read
	attr_reader :block_info

	# Whether or not data bigger than COMPRESSION_MIN_SIZE should be
	# compressed using Zlib when written to this log file. Defaults to true
	attr_predicate :compress?

        # Returns true if +file+ is a valid, up-to-date, pocolog file
        def self.valid_file?(file)
            Logfiles.new(file)
            true
        rescue
            false
        end

        # An array of IO objects representing the underlying files
	attr_reader :io
        # The streams encountered so far. It is initialized the first time
        # #streams gets called
	attr_reader :streams
        # The type registry for these logfiles, as a Typelib::Registry instance
	attr_reader :registry

        # call-seq:
        #   Logfiles.open(io1, io2)
        #   Logfiles.open(io1, io2, registry)
        #
        # This is usually not used directly. Most users want to use Pocolog.open
        # to read existing file(s), and Pocolog.create to create new ones.
        #
        # Creates a new Logfiles object to read the given IO objects. If the
        # last argument is a Typelib::Registry instance, update this registry
        # with the type definitions found in the logfile.
        #
        # Providing a type registry guarantees that you get an error if the
        # logfile's types do not match the type definitions found in the
        # registry.
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
            if io.empty?
                # When opening existing files, @streams is going to be
                # initialized in #streams. However, if we are creating a new set
                # (i.e. io.empty? == true), we also need to tell the system that
                # there currently are no streams available.
                @streams = Array.new
            else
                read_prologue
            end
	end

        # Close the underlying IO objects
	def close
	    io.each { |file| file.close }
	end

	# The basename for creating new log files. The files
	# names are
	#
	#   #{basename}.#{index}.log
	attr_accessor :basename

        # Returns the current position in the current IO
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
                @rio = io_index
                @next_block_pos = pos.block_pos
            else
		raise ArgumentError, "need rio argument, if pos is not a DataHeader" unless rio
		@rio = rio
                @next_block_pos = pos
            end
            nil
        end

	# Continue writing logs in a new file. See #basename to know how
	# files are named
	def new_file(filename = nil)
	    name = filename || "#{basename}.#{@io.size}.log"
	    io = File.new(name, 'w')
	    Logfiles.write_prologue(io)
	    @io << io
	    streams.each_with_index do |s, i|
		write_stream_declaration(i, s.name, s.type.name, registry.to_xml)
	    end
	end

        # Opens a set of file. +pattern+ can be a globbing pattern, in which
        # case all the matching files will be opened as a log sequence
        def self.open(pattern, registry = nil)
            io = Dir.enum_for(:glob, pattern).map { |name| File.open(name) }
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
	def self.create(basename, registry = nil)
            if !registry
                registry = Typelib::Registry.new
                Typelib::Registry.add_standard_cxx_types(registry)
            end
	    file = Logfiles.new(registry)
	    file.basename = basename
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
	    
        # Tries to read the prologue of the underlying files
        #
        # Raises MissingPrologue if no prologue is found, or ObsoleteVersion if
        # the file format is not up-to-date (in which case one has to run
        # pocolog --to-new-format).
	def read_prologue # :nodoc:
	    io = rio
	    io.seek(0)
	    magic	   = io.read(MAGIC.size)
	    if magic != MAGIC
                if !magic
                    raise MissingPrologue, "#{io.path} is empty"
                else
                    raise MissingPrologue, "#{io.path} is not a pocolog log file"
                end
	    end

	    @format_version, big_endian = io.read(9).unpack('xVV')
	    @endian_swap = ((big_endian != 0) ^ Pocolog.big_endian?)
	    if format_version < FORMAT_VERSION
		raise ObsoleteVersion, "old format #{format_version}, current format is #{FORMAT_VERSION}. Convert it using the --to-new-format of pocolog"
	    elsif format_version > FORMAT_VERSION
		raise "this file is in v#{format_version} which is newer that the one we know #{FORMAT_VERSION}. Update pocolog"
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

        # True if we read the last block in the file set
        def eof?; @io.size == @rio end
	# Returns the IO object currently used for reading
	def rio; @io[@rio] end
	# Returns the IO object currently used for writing
	def wio; @io.last end
        # Returns the file size of the IO object currently used
        def file_size; @io_size[@rio] end
	
        # Yields a BlockInfo instance for each block found in the file set.
        #
        # If +rewind+ is true, rewind the file to the first block before
        # iterating.
        #
        # The with_prologue option specifies whether the prologue should be read
        # after rewind. It is meant to be used internally to upgrade old files.
        #
        # This is not meant for direct use. Use #each_data_block instead.
	def each_block(rewind = true, with_prologue = true)
	    self.rewind if rewind
	    while !eof?
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
                handle_block(block_info)
                return block_info
            end
            nil
        end

        # Reads the next data sample in the file, and returns its header.
        # Returns nil if the end of file has been reached. Unlike +next+, it
        # does not decodes the data payload.
	def advance(index)
            each_data_block(index, false) do
                return data_header
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
                    Pocolog.warn "found data block for stream #{block_info.index} but this stream has never been declared, seems Logfile is Corrupted. Skipping..."
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
            if !type || !index || !payload_size
                return
            end

            next_block_pos = rio.tell + payload_size
            if file_size < next_block_pos
                return
            end

            if !BLOCK_TYPES.include?(type)
                file = if rio.respond_to?(:path) then " in file #{rio.path}"
                       end
                Pocolog.warn "invalid block type '#{type}' found#{file} at position #{rio.tell}, expected one of #{BLOCK_TYPES.join(", ")}. The file is probably corrupted. The rest of the file will be ignored."
                return
            end

            @block_info.io           = @rio
            @block_info.pos          = @next_block_pos
            @block_info.type         = type
            @block_info.index        = index
            @block_info.payload_size = payload_size
            @next_block_pos = next_block_pos
            true
        end

	# call-seq:
        #   each_data_block([stream_index[, rewind]]) do |stream_index|
        #   end
        #
        # Yields for each data block in stream +stream_index+, or in all
	# streams if +stream_index+ is nil. The block header can be retrieved
        # using #data_header, and the sample by using #data
        #
        # If +rewind+ is true, starts at the beginning of the file. Otherwise,
        # start at the current position
        #
        # The with_prologue parameter is a backward compatibility feature that
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
            elsif !rio.closed?
		raise $!, "#{$!.message} at position #{rio.pos}", $!.backtrace
	    end
	end

        # Basic information about a stream, as saved in the index files
        class StreamInfo
	    STREAM_INFO_VERSION = "1.2"
	    
	    #the version of the StreamInfo class. This is only used to detect
	    #if an old index file is on the disk compared to the code 
            attr_accessor :version
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

	    # The index data itself. 
	    # This is a instance of StreamIndex
            attr_accessor :index

            # True if this stream is empty
            def empty?; size == 0 end

            def initialize
		@version = STREAM_INFO_VERSION
                @interval_io = []
                @interval_lg = []
                @interval_rt = []
                @size        = 0
                @index       = StreamIndex.new()
            end
        end

        # Load the given index file. Returns nil if the index file does not
        # match the files in the file set.
        def load_index_file(index_filename)
            # Look for an index. If it is found, load it and use it.
            return unless File.readable?(index_filename)
            Pocolog.info "loading file info from #{index_filename}... "
            index_data = File.open(index_filename).read
            file_info, stream_info =
                begin Marshal.load(index_data)
                rescue Exception => e
                    if e.kind_of?(Interrupt)
                        raise
                    else
                        raise InvalidIndex, "cannot unmarshal index data"
                    end
                end

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
		if(!info.respond_to?("version") || info.version != StreamInfo::STREAM_INFO_VERSION || !info.declaration_block)
		    raise InvalidIndex, "old index file found"
		end

                @rio, pos = info.declaration_block
                if read_one_block(pos, @rio).type != STREAM_BLOCK
                    raise InvalidIndex, "invalid declaration_block reference in index"
                end

                # Read the stream declaration block and then update the
                # info attribute of the stream object
                if !info.empty?
                    @rio, pos = info.interval_io[0]
                    if read_one_block(pos, @rio).type != DATA_BLOCK
                        raise InvalidIndex, "invalid start IO reference in index"
                    end

                    if block_info.index != idx
                        raise InvalidIndex, "invalid interval_io: stream index mismatch for #{@streams[idx].name}. Expected #{idx}, got #{data_block_index}."
                    end

		    if !info.index.sane?
                        raise InvalidIndex, "index failed internal sanity check"
		    end

                    @streams[idx].instance_variable_set(:@info, info)
                end
            end
            return @streams.compact

        rescue InvalidIndex => e
            Pocolog.warn "invalid index file #{index_filename}"
	    nil
        end

        # Returns a stream from its index
        def stream_from_index(index)
            @streams[index]
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
            Pocolog.info "building index ..."
	    each_data_block(nil, true) do |stream_index|
                # The stream object itself is built when the declaration block
                # has been found
                s    = @streams[stream_index]
                if s.nil? 
                    Pocolog.warn "Got empty Streamline. Seems file is corrupted, skipping this" 
                else
                    info = s.info
                    info.interval_io[1] = [@rio, block_info.pos]
		    info.interval_io[0] ||= info.interval_io[1]

		    info.index.add_sample_to_index(@rio, data_header.block_pos, data_header.lg)		    
                    info.size += 1
                end
	    end

            if !@streams
                Pocolog.info "done"
                return []
            end

	    @streams.each do |s|
		next unless s

		#set correct time interval
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
            Pocolog.info "done"
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

            # Load the metadata if it seems that there is one
	    unless rio.tell == block_start + block_info.payload_size
		metadata_size = rio.read(4).unpack('V').first
		metadata      = rio.read(metadata_size)
	    end

	    stream_index = block_info.index
	    if @streams && (old = @streams[stream_index])
		unless old.name == name && old.typename == typename || old.registry == registry
		    raise "stream #{name} changed definition"
		end
		old
	    else
		@streams ||= Array.new
		s = (@streams[stream_index] = DataStream.new(self.dup, stream_index, name, typename, registry || '', YAML.load(metadata || '') || Hash.new))

                info = StreamInfo.new
                s.instance_variable_set(:@info, info)
                info.declaration_block = [io_index, block_info.pos]
                s.rewind
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

	DataHeader = Struct.new :io, :block_pos, :payload_pos, :rt, :lg, :size, :compressed, :updated

	# Reads the header of a data block. This sets the @data_header
	# instance variable to a new DataHeader object describing the
	# last read block. If you want to keep a reference on a data block,
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
                @data_header.block_pos   = @block_info.pos
		@data_header.payload_pos = rio.tell
		@data_header.rt = rt
		@data_header.lg = lg
		@data_header.size = data_size
		@data_header.compressed = (compressed != 0)
		@data_header.updated = true
		@data_header
	    end
	end

	def sub_field(offset, size, data_header = nil)
	    data_header ||= self.data_header
	    if data_header.compressed
		raise "field access on compressed files is unsupported"
	    end
	    data_header.io.seek(data_header.payload_pos + offset)
	    data = data_header.io.read(size)
	    data
	end
	
	# Returns the raw data payload of the current block
	def data(data_header = nil)
	    if @data && !data_header then @data
	    else
		data_header ||= self.data_header
		data_header.io.seek(data_header.payload_pos)
		data = data_header.io.read(data_header.size)
		if data_header.compressed
		    # Payload is compressed
		    data = Zlib::Inflate.inflate(data)
		end
                if !data_header
                    @data = data
                end
                data
	    end
	end

        # Formats a block and writes it to +io+
        def self.write_block(wio, type, index, payload)
	    wio << [type, index, payload.size].pack('CxvV')
	    wio << payload
            return wio
        end

        # Write a raw block. +type+ is the block type (either CONTROL_BLOCK,
        # DATA_BLOCK or STREAM_BLOCK), +index+ the stream index for stream and
        # data blocks and the control block type for control blocs. +payload+ is
        # the block's payload.
        def write_block(type,index,payload)
            return Logfiles.write_block(wio,type,index,payload)
        end

        # Encodes and writes a stream declaration block to +wio+
        def self.write_stream_declaration(wio, index, name, type_name, type_registry = nil, metadata = Hash.new)

            if type_name.respond_to?(:name)
                type_registry ||= type_name.registry.minimal(type_name.name).to_xml
                type_name  = type_name.name
            end

            metadata = YAML.dump(metadata)
            payload = [DATA_STREAM, name.size, name, 
                type_name.size, type_name,
                type_registry.size, type_registry,
                metadata.size, metadata
            ].pack("CVa#{name.size}Va#{type_name.size}Va#{type_registry.size}Va#{metadata.size}")
	    write_block(wio, STREAM_BLOCK, index, payload)
        end

        # Helper method that makes sure to create new files if the current file
        # size is bigger than MAX_FILE_SIZE (if defined). 
	def do_write # :nodoc:
            yield
	    
	    if defined?(MAX_FILE_SIZE) && (wio.tell > MAX_FILE_SIZE)
		new_file
	    end
	end

        # Writes a stream declaration to the current write IO
	def write_stream_declaration(index, name, type, registry, metadata)
            do_write do
                Logfiles.write_stream_declaration(wio, index, name, type, registry, metadata)
            end
	end

        # Returns all streams of the given type. The type can be given by its
        # name or through a Typelib::Type subclass
        def streams_from_type(type)
            if type.respond_to?(:name)
                type = type.name
            end

            streams.find_all { |s| s.type.name == type }
        end

        # Returns a stream of the given type, if there is only one. The type can
        # be given by its name or through a Typelib::Type subclass
        #
        # If there is no match or multiple matches, raises ArgumentError.
        def stream_from_type(type)
            matches = streams_from_type(type)
            if matches.empty?
                raise ArgumentError, "there is no stream in this file with the required type"
            elsif matches.size > 1
                raise ArgumentError, "there is more than one stream in this file with the required type"
            else
                return matches.first
            end
        end

        # Explicitely creates a new stream named +name+, of the given type and
        # metadata
        def create_stream(name, type, metadata = Hash.new)
            if type.respond_to?(:to_str)
                type = registry.get(type)
            end

	    typename  = type.name
            registry = type.registry.minimal(type.name).to_xml

	    @streams ||= Array.new
	    new_index = @streams.size
	    write_stream_declaration(new_index, name, type.name, registry, metadata)

	    stream = DataStream.new(self, new_index, name, typename, registry, metadata)
	    @streams << stream
	    stream
        end

	# Returns the DataStream object for +name+, +registry+ and
	# +type+. Optionally creates it.
        #
        # If +create+ is false, raises ArgumentError if the stream does not
        # exist.
	def stream(name, type = nil, create = false)
	    if s = streams.find { |s| s.name == name }
                s.registry # load the registry NOW
		return s
	    elsif !type || !create
		raise ArgumentError, "no such stream #{name}"
	    end
            create_stream(name, type)
	end

        # Returns true if +name+ is the name of an existing stream
        def has_stream?(name)
            !!stream(name)
        rescue ArgumentError
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
        # Formatting string for Array.pack to create a data block
	DATA_BLOCK_HEADER_FORMAT = "VVx#{TIME_PADDING}VVx#{TIME_PADDING}VC"

        def self.write_data_block(io, stream_index, rt, lg, compress, data)
            payload = [rt.tv_sec, rt.tv_usec, lg.tv_sec, lg.tv_usec,
                data.length, compress, data
            ].pack("#{DATA_BLOCK_HEADER_FORMAT}a#{data.size}")
            write_block(io, DATA_BLOCK, stream_index, payload)
        end

        # Write a data block for stream index +stream+, with the provided times
        # and the given data. +data+ must already be marshalled (i.e. it is
        # meant to be a String that represents a byte array).
	def write_data_block(stream, rt, lg, data) # :nodoc:
	    compress = 0
	    if compress? && data.size > COMPRESSION_MIN_SIZE
		data = Zlib::Deflate.deflate(data)
		compress = 1
	    end

            do_write do
                Logfiles.write_data_block(wio, stream.index, rt, lg, compress, data)
            end
	end
    end

    # Returns the stream called +stream_name+ from file
    def self.file_stream(file_name, stream_name)
        file = Logfiles.open(file_name)
        file.stream(stream_name)
    end
end

