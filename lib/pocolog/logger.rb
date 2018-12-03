module Pocolog
    class Logger
        Sampling = Struct.new :next_sample, :stream, :source, :period, :last_write

        attr_reader :mutex
        attr_reader :output
        attr_reader :on_demand
        attr_reader :periodic

        # Create a new logger which will log into the +output+ Logfiles object.
        def initialize(output)
            @mutex = Mutex.new 
            @on_demand = []
            @periodic  = []
            @output = output
        end

        def close
            @quit = true
            @logging_thread.join if @logging_thread
            output.close
        end

        # Add a new source to load
        def add(source, period)
            stream = output.stream source.full_name, source.type, true

            spec = Sampling.new(nil, stream, source, nil)
            if period == :on_demand
                on_demand << spec
            else 
                mutex.synchronize do
                    spec.period      = Float(period)
                    spec.next_sample = Time.now + spec.period
                    periodic << spec
                end
            end
        end

        def start
            @logging_thread = Thread.new do
                while true
                    break if @quit
                    log_pending_sources
                    sleep(periodic.first.next_sample - Time.now)
                end
            end
        end

        def log_pending_sources
            while true
                # Loop until there is no pending sample to write
                mutex.synchronize do
                    @periodic = periodic.sort_by { |spec| spec.next_sample }
                    next_read = periodic.first
                    return if next_read.next_sample > Time.now
                end

                log_source(next_read)
                next_read.next_sample += next_read.period
            end
        end

        def log_sample(spec, force = false)
            source = spec.source
            if force || (source.timestamp != spec.last_write)
                spec.stream.write(Time.now, source.timestamp, source.read)
                spec.last_write = source.timestamp
            end
        end


        # Log all configured sources once, now.  If +force+ is true, read even
        # if the sources have not been updated
        def now(force = false)
            mutex.synchronize do
                (on_demand + periodic).each do |spec|
                    log_sample(spec, force)
                end
            end

        end
    end
end

