require 'utilrb/logger'

module Pocolog
    # Class that encapsulates the logic of upgrading a file or a set of file(s)
    class FileUpgrader
        extend Logger::Hierarchy
        include Logger::Hierarchy
        include Logger::Forward

        attr_reader :loader
        attr_reader :converter_registry

        def initialize(loader = OroGen::Loaders::PkgConfig.new('gnulinux'))
            @loader = loader
            @converter_registry = Upgrade::ConverterRegistry.new
        end

        def upgrade(in_logfile, out_logfile, reporter: CLI::NullReporter.new, skip_failures: false)
            has_block = block_given?
            in_logfile.streams.each do |stream|
                begin
                    target_typekit = loader.typekit_for(stream.type.name, false)
                    target_type = target_typekit.resolve_type(stream.type.name)
                rescue OroGen::NotTypekitType
                    target_type = stream.type
                end

                if stream.empty?
                    reporter.log "defining empty stream #{stream.name}"
                    # Just create the new stream
                    out_logfile.create_stream stream.name, target_type, stream.metadata
                    next
                end

                stream_ref_time = stream.time_interval(true).first
                begin
                    ops = Upgrade.compute(stream_ref_time, stream.type, target_type, converter_registry)
                rescue Upgrade::InvalidCast => e
                    if skip_failures
                        reporter.warn "cannot upgrade #{stream.name} of #{in_logfile.rio.path}"
                        PP.pp(e, buffer = "")
                        buffer.split("\n").each do |line|
                            reporter.warn line
                        end
                        next
                    else
                        raise e, "cannot upgrade #{stream.name} of #{in_logfile.rio.path}: #{e.message}", e.backtrace
                    end
                end

                target_stream = out_logfile.create_stream(stream.name, target_type, stream.metadata)

                if ops.identity?
                    reporter.log "copying stream #{stream.name}"
                    data_buffer = String.new
                    sample_i = 0
                    while header = stream.advance
                        in_logfile.data(header, data_buffer)
                        target_stream.write_raw(header.rt_time, header.lg_time, data_buffer)

                        if sample_i % 10_000 == 0
                            reporter.advance(10_000)
                        end
                        sample_i += 1
                    end
                else
                    reporter.log "upgrading stream #{stream.name}"
                    sample_i = 0
                    data_buffer = String.new
                    source_sample = stream.type.new
                    target_sample = target_type.new
                    while header = stream.advance
                        stream.raw_data(header, source_sample)
                        ops.call(target_sample, source_sample)
                        target_stream.write(header.rt, header.lg, target_sample)

                        if sample_i % 10_000 == 0
                            reporter.advance(10_000)
                        end
                        sample_i += 1
                    end
                end
            end
        end
    end
end

