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

        attr_reader :loader
        attr_reader :converter_registry

        def initialize(loader)
            @loader = loader
            @converter_registry = Upgrade::ConverterRegistry.new
        end

        StreamCopy = Struct.new :in_stream, :out_type, :ops do
            def copy?; ops.identity? end
        end

        # Upgrade the data in the streams of a logfile
        #
        # @param [Pocolog::Logfiles] in_logfile the logfile whose streams should
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
        def upgrade(in_logfile, out_path, reporter: CLI::NullReporter.new, skip_failures: false, reflink: false)
            stream_copy = compute_stream_copy(in_logfile)

            if reflink && can_cp?(in_logfile, stream_copy)
                cp_logfile_and_index(in_logfile, out_path, reporter: reporter)
                return
            end

            out_logfile = Pocolog::Logfiles.new(Typelib::Registry.new)
            out_logfile.new_file(out_path)

            copied_samples = 0
            raw_stream_info = stream_copy.map do |copy|
                in_stream = copy.in_stream
                out_stream_pos = out_logfile.io.tell
                out_stream = out_logfile.create_stream(in_stream.name, copy.out_type, in_stream.metadata)

                if copy.ops.identity?
                    reporter.log "copying stream #{in_stream.name}"
                    copied_samples, index_map =
                        copy_stream(in_stream, out_stream, reporter: reporter)
                else
                    reporter.log "upgrading stream #{in_stream.name}"
                    copied_samples, index_map =
                        upgrade_stream(copy.ops, in_stream, out_stream, reporter: reporter)
                end
                reporter.base += copied_samples
                Pocolog::IndexBuilderStreamInfo.new(out_stream_pos, index_map)
            end
            out_logfile.flush
            out_logfile.close

            block_stream = Pocolog::BlockStream.open(out_path)
            stream_info = Pocolog.create_index_from_raw_info(block_stream, raw_stream_info)
            File.open(Pocolog::Logfiles.default_index_filename(out_path), 'w') do |io|
                Pocolog::Format::Current.write_index(io, block_stream.io, stream_info)
            end
        rescue Exception
            if out_logfile
                out_logfile.close
                FileUtils.rm_f(out_path)
            end
            raise
        end

        # @api private
        #
        # Resolve the local version of the given type
        #
        # This is the type that will be used as target for a stream upgrade
        def resolve_local_type(in_type)
            target_typekit = loader.typekit_for(in_type.name, false)
            target_typekit.resolve_type(in_type.name)
        rescue OroGen::NotTypekitType
            in_stream.type
        end

        # @api private
        #
        # Compute the stream copy operations for each stream in the logfile
        #
        # @param [Boolean] skip_failures if true, streams that cannot be
        #   upgraded will be ignored. If false, the method raises Upgrade::InvalidCast
        #
        # @return [Array<StreamCopy>]
        def compute_stream_copy(in_logfile, reporter: CLI::NullReporter.new, skip_failures: false)
            in_logfile.streams.map do |in_stream|
                out_type = resolve_local_type(in_stream.type)

                if in_stream.empty?
                    next(StreamCopy.new(in_stream, out_type, Upgrade::Ops::Identity.new(out_type)))
                end

                stream_ref_time = in_stream.time_interval(true).first
                begin
                    ops = Upgrade.compute(stream_ref_time, in_stream.type, out_type, converter_registry)
                rescue Upgrade::InvalidCast => e
                    if skip_failures
                        reporter.warn "cannot upgrade #{in_stream.name} of #{in_logfile.path}"
                        PP.pp(e, buffer = "")
                        buffer.split("\n").each do |line|
                            reporter.warn line
                        end
                        next
                    else
                        raise e, "cannot upgrade #{in_stream.name} of #{in_logfile.path}: #{e.message}", e.backtrace
                    end
                end
                StreamCopy.new(in_stream, out_type, ops)
            end.compact
        end

        # @api private
        #
        # Check if the input file can be copied as-is
        def can_cp?(in_logfile, stream_copy)
            (in_logfile.num_io == 1) && stream_copy.all? { |s| s.copy? }
        end

        # @api private
        #
        # Copies the logfile and its index to the out path
        def cp_logfile_and_index(in_logfile, out_path, reporter: CLI::NullReporter.new)
            in_path = in_logfile.path
            if FileUtils.cp_reflink(in_path, out_path)
                reporter.log "file dos not require an upgrade, copied (with reflink)"
            else
                reporter.log "file dos not require an upgrade, copied (without reflink)"
            end

            in_idx_path  = File.join(File.dirname(in_path), File.basename(in_path, '.0.log') + '.0.idx')
            out_idx_path = File.join(File.dirname(out_path), "#{File.basename(out_path, '.0.log')}.0.idx")
            if File.file?(in_idx_path)
                FileUtils.cp_reflink(in_idx_path, out_idx_path)
            end
        end

        # Copy a stream
        def copy_stream(in_stream, out_stream, reporter: CLI::NullReporter.new, report_period: 0.1)
            in_logfile = in_stream.logfile
            sample_i = 0
            last_report = Time.now
            index_map = Array.new
            out_io = out_stream.logfile.io
            while header = in_stream.advance
                buffer = in_logfile.data(header)
                out_pos = out_io.pos
                out_stream.write_raw(header.rt_time, header.lg_time, buffer)
                index_map << out_pos << header.lg_time

                now = Time.now
                if now - last_report > report_period
                    reporter.current = sample_i
                    last_report = now
                end
                sample_i += 1
            end
            return sample_i, index_map
        end

        # Process a stream through an upgrade operation
        def upgrade_stream(ops, in_stream, out_stream, reporter: CLI::NullReporter.new, report_period: 0.1)
            in_type  = in_stream.type
            out_type = out_stream.type

            sample_i = 0
            last_report = Time.now
            index_map = Array.new
            out_io = out_stream.logfile.io
            while header = in_stream.advance
                in_sample  = in_type.new
                out_sample = out_type.new
                in_stream.raw_data(header, in_sample)
                ops.call(out_sample, in_sample)
                out_pos = out_io.tell
                out_stream.write_raw(header.rt_time, header.lg_time, out_sample.to_byte_array)
                index_map << out_pos << header.lg_time

                now = Time.now
                if now - last_report > report_period
                    reporter.current = sample_i
                    last_report = now
                end
                sample_i += 1
            end
            return sample_i, index_map
        end
    end
end

