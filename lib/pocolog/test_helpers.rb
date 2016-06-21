require 'tmpdir'

module Pocolog
    module TestHelpers
        # For convenience, the test helpers create a int32_t type and use it by
        # default
        attr_reader :int32_t

        def setup
            @__logfiles_dir = Dir.mktmpdir
            registry = Typelib::Registry.new
            @int32_t = registry.create_numeric '/int32_t', 4, :sint
            super
        end

        def teardown
            FileUtils.rm_rf @__logfiles_dir
            super
        end

        # Create a new logfile
        #
        # @param [String] filename the path to the log file
        # @param [Hash<String,Array<Integer>>] a set of streams that should be
        #   created. All streams are created with a type of /int32_t (32 bit,
        #   integer, signed). A sample time is always 100 * sample_index + value 
        def create_logfile(basename)
            @__current_logfile = Pocolog::Logfiles.create(logfile_path(basename))

            if block_given?
                begin yield
                ensure close_logfile
                end
            end
        end

        def flush_logfile
            @__current_logfile.flush
        end

        # Close the current logfile (either created or opened)
        def close_logfile
            if @__current_logfile
                @__current_logfile.close
                logfile, @__current_logfile = @__current_logfile, nil
                logfile
            end
        end

        # Create a stream on the last created logfile
        def create_logfile_stream(name, samples_rt = Array.new, samples_lg = Array.new, samples_value = Array.new, type: int32_t, metadata: Hash.new)
            stream = @__current_logfile.create_stream(name, type, metadata)
            samples_rt.zip(samples_lg, samples_value).each do |rt, lg, v|
                stream.write(rt, lg, v)
            end
            @__current_stream = stream
            stream
        end

        def write_logfile_sample(rt, lg, value)
            @__current_stream.write(rt, lg, value)
        end

        # Open an existing logfile
        def open_logfile(path, close_current: true)
            if @__current_logfile
                close_logfile
            end

            logfile = Pocolog::Logfiles.open(logfile_path(path))
            if block_given?
                begin yield(logfile)
                ensure close_logfile
                end
            else
                logfile
            end
        end

        def open_logfile_stream(basename, stream_name, close_current: true)
            open_logfile(basename, close_current: close_current).
                stream(stream_name)
        end

        def logfile_path(basename)
            File.expand_path(basename, @__logfiles_dir)
        end
    end
end
