require 'utilrb/module/attr_predicate'
require 'yaml'
require 'fileutils'
module Pocolog
    class InternalError < RuntimeError; end

    class InvalidFile < RuntimeError; end
    class InvalidBlockFound < InvalidFile; end
    class NotEnoughData < InvalidBlockFound; end

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
	MAGIC = Format::Current::MAGIC
        FORMAT_VERSION    = Format::Current::VERSION
        BLOCK_HEADER_SIZE = Format::Current::BLOCK_HEADER_SIZE
        TIME_SIZE         = Format::Current::TIME_SIZE

	# Data blocks of less than COMPRESSION_MIN_SIZE are never compressed
	COMPRESSION_MIN_SIZE = 60 * 1024
	# If the size gained by compressing is below this value, do not save in
	# compressed form
	COMPRESSION_THRESHOLD = 0.3

        # Whether the data stored in this logfile is in big endian or little
        # endian
	attr_predicate :big_endian?

	# Whether or not data bigger than COMPRESSION_MIN_SIZE should be
	# compressed using Zlib when written to this log file. Defaults to true
	attr_predicate :compress?, true

        # Whether the endianness of the data stored in the file matches the
        # host's (false) or not (true)
        def endian_swap; big_endian? ^ Pocolog.big_endian? end

        # Returns true if +file+ is a valid, up-to-date, pocolog file
        def self.valid_file?(file)
            Format::Current.valid_file?(file)
        end

        # The underlying IO object
        #
        # Sequence of files are handled through the {IOSequence} facade
	attr_reader :io
        # The type registry for these logfiles, as a Typelib::Registry instance
	attr_reader :registry
	# Set of data streams found in this file
	attr_reader :streams

        # The block stream object used to interpret the data stream
        attr_reader :block_stream

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
	def initialize(*io, write_only: false, index_dir: nil, silent: false)
	    if io.last.kind_of?(Typelib::Registry)
		@registry = io.pop
	    end

            @path = io.first.path if !io.empty?
            @io =
                if io.size == 1
                    io.first
                else
                    IOSequence.new(*io)
                end
            @block_stream = BlockStream.new(@io)
            @big_endian = block_stream.big_endian?

            @data = nil
	    @streams     = nil
	    @compress    = true
            @data_header_buffer = ""
            if !write_only
                @streams = load_stream_info(io, index_dir: index_dir, silent: silent)
            else
                @streams = Array.new
            end
	end

        attr_reader :path

        def num_io
            if io.respond_to?(:num_io)
                io.num_io
            else
                1
            end
        end

        def closed?
            io.closed?
        end

        # Flush the IO objects
	def flush
	    io.flush
	end

        # Close the underlying IO objects
	def close
	    io.close
	end

        def open
            io.open
        end

        # True if we read the last block in the file set
        def eof?
            io.eof?
        end

	# The basename for creating new log files. The files
	# names are
	#
	#   #{basename}.#{index}.log
	attr_accessor :basename

        # Returns the current position in the current IO
        #
        # This is the position of the next block.
	def tell
            block_stream.tell
        end

	# Continue writing logs in a new file. See #basename to know how
	# files are named
	def new_file(filename = nil)
	    name = filename || "#{basename}.#{num_io}.log"
	    io = File.new(name, 'w+')
	    Format::Current.write_prologue(io)
	    streams.each_with_index do |s, i|
                Logfiles.write_stream_declaration(io, i, s.name, s.type.name, s.type.to_xml, s.metadata)
	    end
            if num_io == 0
                @io = io
            elsif num_io == 1
                @io = IOSequence.new(@io, io)
            else
                @io.add_io(io)
            end
            @block_stream = BlockStream.new(@io)
	end

        # Opens a set of file. +pattern+ can be a globbing pattern, in which
        # case all the matching files will be opened as a log sequence
        def self.open(pattern, registry = Typelib::Registry.new, index_dir: nil, silent: false)
            io = Dir.enum_for(:glob, pattern).sort.map { |name| File.open(name) }
            if io.empty?
                raise ArgumentError, "no files matching '#{pattern}'"
            end

            new(*io, registry, index_dir: index_dir, silent: silent)
        end

	# Create an empty log file using +basename+ to build its name.
        # Namely, it will create a new file named <basename>.0.log. Then,
        # calls to #new_file would create <basename>.1.log and so on
	def self.create(basename, registry = Typelib::Registry.new)
	    file = Logfiles.new(registry, write_only: true)
            if basename =~ /\.\d+\.log$/
                file.new_file(basename)
            else
                file.basename = basename
                file.new_file
            end
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

	    @io		 = from.io.dup
            @block_stream = BlockStream.new(@io)
            @registry    = from.registry.dup
	end

        # Returns the default index file for a given file
        #
        # @param [String] path the log file's path
        # @return [String] the index file name
        def self.default_index_filename(path, index_dir: File.dirname(path))
            index_filename = File.basename(path).gsub(/\.log$/, '.idx')
            index_path = File.join(index_dir, index_filename)
            if index_path == path
                raise ArgumentError, "#{path} does not end in .log, cannot generate a default index name for it"
            end
            index_path
        end

        # Load stream information, either from an existing on-disk index or by
        # rebuilding an index
        #
        # @return [Array<DataStream,nil>]
        def load_stream_info(ios, index_dir: nil, silent: false)
            per_file_stream_info = ios.map do |single_io|
                load_stream_info_from_file(single_io, index_dir: (index_dir || File.dirname(single_io)), silent: silent)
            end
            stream_count = per_file_stream_info.map(&:size).max || 0
            per_stream = ([nil] * stream_count).zip(*per_file_stream_info)

            streams = Array.new
            per_stream.each_with_index do |(_, *stream_info), stream_index|
                combined_info = StreamInfo.new
                file_pos_offset = 0
                stream_block = nil
                stream_info.each_with_index do |(block, info), file_index|
                    stream_block ||= block
                    if info
                        combined_info.concat(info, file_pos_offset)
                    end
                    file_pos_offset += ios[file_index].size
                end

                if stream_block
                    streams[stream_index] = 
                        DataStream.new(self, stream_index, stream_block.name, stream_block.type,
                                       stream_block.metadata, combined_info)
                end
            end
            streams
        end

        # Get the index for the file backed by the given IO
        #
        # @param [File] io the file. It must be a single file
        # @return [Array<StreamInfo>] list of
        #   streams in the given file
        def load_stream_info_from_file(io, index_dir: File.dirname(io.path), silent: false)
            index_filename = self.class.default_index_filename(io.path, index_dir: index_dir)
            if File.exist?(index_filename)
                Pocolog.info "loading file info from #{index_filename}... " if !silent
                begin
                    streams_info = File.open(index_filename) do |index_io|
                        Format::Current.read_index(index_io, expected_file_size: io.size, expected_mtime: nil)
                    end
                    return initialize_from_stream_info(io, streams_info)
                rescue InvalidIndex => e
                    Pocolog.warn "invalid index file #{index_filename}: #{e.message}" if !silent
                end
            end
            return rebuild_and_load_index(io, index_filename, silent: silent)
        end

        # @api privat
        #
        # Initialize self by loading information from an index file
        def initialize_from_stream_info(io, stream_info)
            block_stream = BlockStream.new(io)

            streams = Array.new
            stream_info.each_with_index do |info, idx|
                pos = info.declaration_blocks.first
                block_stream.seek(pos)
                if block_stream.read_next_block_header.kind != STREAM_BLOCK
                    raise InvalidIndex, "invalid declaration_block reference in index"
                end
                stream_block = block_stream.read_stream_block

                # Read the stream declaration block and then update the
                # info attribute of the stream object
                if !info.empty?
                    pos = info.interval_io[0]
                    block_stream.seek(pos)
                    block_info = block_stream.read_next_block_header
                    if block_info.kind != DATA_BLOCK
                        raise InvalidIndex, "invalid start IO reference in index"
                    elsif block_info.stream_index != idx
                        raise InvalidIndex, "invalid interval_io: stream index mismatch"
                    end
                end

                streams[idx] = [stream_block, info]
            end
            streams
        end

        # Go through the whole file to extract index information, and write the
        # index file
        def rebuild_and_load_index(io, index_path = self.class.default_index_filename(io), silent: false)
            # No index file. Compute it.
            Pocolog.info "building index #{io.path} ..." if !silent
            io.rewind
            block_stream = BlockStream.new(io)
            block_stream.read_prologue
            stream_info = Pocolog.file_index_builder(block_stream)
            FileUtils.mkdir_p(File.dirname(index_path))
            File.open(index_path, 'w') do |index_io|
                Format::Current.write_index(index_io, io, stream_info)
            end
            io.rewind
            Pocolog.info "done" if !silent
            initialize_from_stream_info(io, stream_info)
        end

        # Returns a stream from its index
        def stream_from_index(index)
            @streams[index]
        end

	# True if there is a stream +index+
	def declared_stream?(index)
	    @streams && (@streams.size > index && @streams[index]) 
	end

        # Read the block information for the block at a certain position in the
        # IO
        def read_one_block(file_pos)
            block_stream.seek(file_pos)
            header = block_stream.read_next_block_header
            @block_pos = file_pos
            @data = nil
            @data_header = nil
            header
        end

        # Read the data payload for a data block present at a certain position
        # in the IO
        def read_one_data_payload(file_pos)
            read_one_block(file_pos)
            block_stream.read_data_block_payload
        end

        class DataHeader < BlockStream::DataBlockHeader
            attr_accessor :block_pos
            attr_accessor :payload_pos
            attr_accessor :size
        end

	# Reads the header of a data block. This sets the @data_header
	# instance variable to a new DataHeader object describing the
	# last read block. If you want to keep a reference on a data block,
	# and read it later, do the following
	#
	#   block = file.data_header.dup
	#   [do something, including reading the file]
	#   data  = file.data(block)
	def data_header
            if !@data_header
                raw_header = block_stream.read_data_block_header
                h = DataHeader.new(
                    raw_header.rt_time, raw_header.lg_time, raw_header.data_size, raw_header.compressed?)
                h.block_pos   = @block_pos
                h.payload_pos = block_stream.tell
                @data_header = h
            end
            @data_header
	end

        def sub_field(offset, size, data_header = self.data_header)
	    if data_header.compressed
		raise "field access on compressed files is unsupported"
	    end
	    block_stream.seek(data_header.payload_pos + offset)
	    block_stream.read(size)
	end

	# Returns the raw data payload of the current block
	def data(data_header = nil)
	    if @data && !data_header then @data
	    else
		data_header ||= self.data_header
		block_stream.seek(data_header.payload_pos)
		data = block_stream.read(data_header.data_size)
		if data_header.compressed?
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
            wio.write [type, index, payload.size].pack('CxvV')
	    wio.write payload
            return wio
        end

        # Write a raw block. +type+ is the block type (either CONTROL_BLOCK,
        # DATA_BLOCK or STREAM_BLOCK), +index+ the stream index for stream and
        # data blocks and the control block type for control blocs. +payload+ is
        # the block's payload.
        def write_block(type,index,payload)
            return Logfiles.write_block(io,type,index,payload)
        end

        def self.normalize_metadata(metadata)
            result = Hash.new
            metadata.each do |k, v|
                result[k.to_str] = v
            end
            result
        end

        # Encodes and writes a stream declaration block to +wio+
        def self.write_stream_declaration(wio, index, name, type_name, type_registry = nil, metadata = Hash.new)
            if type_name.respond_to?(:name)
                type_registry ||= type_name.registry.minimal(type_name.name).to_xml
                type_name  = type_name.name
            end

            metadata = normalize_metadata(metadata)
            metadata = YAML.dump(metadata)
            payload = [DATA_STREAM, name.size, name, 
                type_name.size, type_name,
                type_registry.size, type_registry,
                metadata.size, metadata
            ].pack("CVa#{name.size}Va#{type_name.size}Va#{type_registry.size}Va#{metadata.size}")
	    write_block(wio, STREAM_BLOCK, index, payload)
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

            registry = type.registry.minimal(type.name).to_xml

	    @streams ||= Array.new
	    new_index = @streams.size
            pos = io.tell
            Logfiles.write_stream_declaration(io, new_index, name, type.name, registry, metadata)

	    stream = DataStream.new(self, new_index, name, type, metadata)
            stream.info.declaration_blocks << pos
	    @streams << stream
	    stream
        end

	# Returns the DataStream object for +name+, +registry+ and
	# +type+. Optionally creates it.
        #
        # If +create+ is false, raises ArgumentError if the stream does not
        # exist.
	def stream(name, type = nil, create = false)
	    if matching_stream = streams.find { |s| s.name == name }
		return matching_stream
	    elsif !type || !create
		raise ArgumentError, "no such stream #{name}"
            else
                Pocolog.warn_deprecated "the 'create' flag of #stream is deprecated, use #create_stream directly instead"
                create_stream(name, type)
	    end
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
            if rt.kind_of?(Time)
                payload = [rt.tv_sec, rt.tv_usec, lg.tv_sec, lg.tv_usec,
                    data.length, compress, data]
            else
                payload = [rt / 1_000_000, rt % 1_000_000, lg / 1_000_000, lg % 1_000_000,
                           data.length, compress, data]
            end
            payload = payload.pack("#{DATA_BLOCK_HEADER_FORMAT}a#{data.size}")
            write_block(io, DATA_BLOCK, stream_index, payload)
        end

        # Write a data block for stream index +stream+, with the provided times
        # and the given data. +data+ must already be marshalled (i.e. it is
        # meant to be a String that represents a byte array).
	def write_data_block(stream, rt, lg, data) # :nodoc:
	    compress = 0
	    if compress? && data.size > COMPRESSION_MIN_SIZE
                raise
		data = Zlib::Deflate.deflate(data)
		compress = 1
	    end

            Logfiles.write_data_block(io, stream.index, rt, lg, compress, data)
	end

        # Creates a stream aligner on all streams of this logfile
        def stream_aligner(use_rt = false)
            StreamAligner.new(use_rt, *streams.compact)
        end
    end

    # Returns the stream called +stream_name+ from file
    def self.file_stream(file_name, stream_name)
        file = Logfiles.open(file_name)
        file.stream(stream_name)
    end
end

