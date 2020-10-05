require 'utilrb/logger'
require 'pocolog/cli/null_reporter'
require 'pocolog/cp_cow'
require 'pocolog/upgrade'

module Pocolog
    # Class that encapsulates the logic of upgrading a file or a set of file(s)
    class FileUpgrader
        extend Logger::Hierarchy
        include Logger::Hierarchy
        include Logger::Forward

        attr_reader :converter_registry

        attr_reader :out_type_resolver

        def initialize(out_type_resolver)
            @out_type_resolver = out_type_resolver
            @converter_registry = Upgrade::ConverterRegistry.new
        end

        StreamCopy = Struct.new :in_stream, :out_type, :ops do
            def copy?; ops.identity? end
        end

        # Upgrade the data in the streams of a logfile
        #
        # @param [Logfiles] in_logfile the logfile whose streams should
        #    be updated
        # @param [String] out_path the path to the file that should be created
        # @param [Boolean] skip_failures if true, streams that cannot be
        #   upgraded will be skipped
        # @param [Boolean] reflink whether we should attempt to CoW the input
        #   files into the target file if they do not need any upgrade. This is
        #   experimental and is therefore disabled by default. Should work only
        #   only Linux and btrfs/zfs.
        # @param [CLI::NullReporter] reporter a reporter that allows to display
        #   some progress information
        def upgrade(in_path, out_path, reporter: CLI::NullReporter.new, skip_failures: false, reflink: false)
            stream_copy = compute_stream_copy(in_path)

            if reflink && can_cp?(stream_copy)
                cp_logfile_and_index(in_path, out_path, reporter: reporter)
                return
            end

            wio = File.open(out_path, 'w+')
            Format::Current.write_prologue(wio)

            stream_ops = Array.new
            stream_pos = Array.new
            stream_types = Array.new
            stream_index_map = Array.new

            stream_copy.each do |copy_info|
                in_stream = copy_info.in_stream
                index = in_stream.index
                stream_ops[index] = copy_info.ops
                stream_pos[index] = wio.tell
                stream_index_map[index] = Array.new
                stream_types[index] = [in_stream.type.new, copy_info.out_type]
                if copy_info.ops.identity?
                    reporter.log "copying #{in_stream.name}"
                else
                    reporter.log "updating #{in_stream.name}"
                end
                Logfiles.write_stream_declaration(
                    wio, index, in_stream.name, copy_info.out_type,
                    nil, in_stream.metadata)
            end

            report_period = 0.1
            last_report = Time.now

            block_stream = BlockStream.open(in_path)
            block_stream.read_prologue
            while block = block_stream.read_next_block_header
                if block.kind == DATA_BLOCK
                    index = block.stream_index
                    ops   = stream_ops[index]
                    block_pos   = wio.tell
                    payload_header, in_marshalled_sample =
                        block_stream.read_data_block(uncompress: false)
                    data_header = BlockStream::DataBlockHeader.parse(payload_header)

                    if ops.identity?
                        wio.write block.raw_data
                        wio.write payload_header
                        wio.write in_marshalled_sample
                    else
                        if data_header.compressed?
                            in_marshalled_sample = Zlib::Inflate.inflate(in_marshalled_sample)
                        end
                        in_sample, out_type = stream_types[index]
                        in_sample.from_buffer_direct(in_marshalled_sample)
                        out_sample = out_type.new
                        ops.call(out_sample, in_sample)
                        out_marshalled_sample = out_sample.to_byte_array
                        out_payload_size = out_marshalled_sample.size
                        payload_header[-5, 4] = [out_payload_size].pack("V")
                        block.raw_data[-4, 4] = [payload_header.size + out_payload_size].pack("V")
                        wio.write block.raw_data
                        wio.write payload_header
                        wio.write out_marshalled_sample
                    end
                    stream_index_map[index] << block_pos << data_header.lg_time
                end
                if Time.now - last_report > report_period
                    reporter.current = block_stream.tell
                    last_report = Time.now
                end
            end

            wio.flush
            wio.rewind
            block_stream = BlockStream.new(wio)
            raw_stream_info = stream_pos.each_with_index.map do |block_pos, stream_i|
                IndexBuilderStreamInfo.new(block_pos, stream_index_map[stream_i])
            end
            stream_info = Pocolog.create_index_from_raw_info(block_stream, raw_stream_info)
            File.open(Logfiles.default_index_filename(out_path), 'w') do |io|
                Format::Current.write_index(io, block_stream.io, stream_info)
            end
        rescue Exception
            FileUtils.rm_f(out_path)
            raise
        ensure
            wio.close if wio && !wio.closed?
        end

        # @api private
        #
        # Compute the stream copy operations for each stream in the logfile
        #
        # @param [Boolean] skip_failures if true, streams that cannot be
        #   upgraded will be ignored. If false, the method raises Upgrade::InvalidCast
        #
        # @return [Array<StreamCopy>]
        def compute_stream_copy(in_path, reporter: CLI::NullReporter.new, skip_failures: false)
            in_logfile = Pocolog::Logfiles.open(in_path)
            in_logfile.streams.map do |in_stream|
                out_type = out_type_resolver.call(in_stream.type)

                if in_stream.empty?
                    next(StreamCopy.new(in_stream, out_type, Upgrade::Ops::Identity.new(out_type)))
                end

                stream_ref_time = in_stream.interval_rt.first
                begin
                    ops = Upgrade.compute(stream_ref_time, in_stream.type, out_type, converter_registry)
                rescue Upgrade::InvalidCast => e
                    if skip_failures
                        reporter.warn "cannot upgrade #{in_stream.name} of #{in_path}"
                        PP.pp(e, buffer = "")
                        buffer.split("\n").each do |line|
                            reporter.warn line
                        end
                        next
                    else
                        raise e, "cannot upgrade #{in_stream.name} of #{in_path}: #{e.message}", e.backtrace
                    end
                end
                StreamCopy.new(in_stream, out_type, ops)
            end.compact
        ensure
            in_logfile.close if in_logfile
        end

        # @api private
        #
        # Check if the input file can be copied as-is
        def can_cp?(stream_copy)
            stream_copy.all? { |s| s.copy? }
        end

        # @api private
        #
        # Copies the logfile and its index to the out path
        def cp_logfile_and_index(in_path, out_path, reporter: CLI::NullReporter.new)
            strategy = FileUtils.cp_cow(in_path, out_path)
            reporter.log "file dos not require an upgrade, copied (#{strategy})"

            in_idx_path  = File.join(File.dirname(in_path), File.basename(in_path, '.0.log') + '.0.idx')
            out_idx_path = File.join(File.dirname(out_path), "#{File.basename(out_path, '.0.log')}.0.idx")
            if File.file?(in_idx_path)
                FileUtils.cp_cow(in_idx_path, out_idx_path)
            end
        end
    end
end

