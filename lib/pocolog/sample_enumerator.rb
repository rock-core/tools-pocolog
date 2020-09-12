module Pocolog
    # Sample enumerators are nicer interfaces for data reading built on top of a DataStream
    # object
    class SampleEnumerator
        include Enumerable

        attr_accessor :use_rt
        attr_accessor :min_time,  :max_time,  :every_time
        attr_accessor :min_index, :max_index, :every_index
        attr_accessor :max_count
        def setvar(name, val)
            time, index = case val
                          when Integer then [nil, val]
                          when Time then [val, nil]
                          end

            send("#{name}_time=", time)
            send("#{name}_index=", index)
        end

        attr_reader :stream, :read_data
        def initialize(stream, read_data)
            @stream = stream
            @read_data = read_data
        end
        def every(interval)
            setvar('every', interval)
            self
        end
        def from(from);
            setvar("min", from)
            self
        end
        def to(to)
            setvar("max", to)
            self
        end
        def between(from, to)
            self.from(from)
            self.to(to)
        end
        def at(pos)
            from(pos)
            max(1)
        end

        def realtime(use_rt = true)
            @use_rt = use_rt
            self
        end

        def max(count)
            @max_count = count
            self
        end

        attr_reader :sample_count

        def each(&block)
            return enum_for(__method__) unless block

            raw_each do |rt, lg, raw_data|
                yield(rt, lg, Typelib.to_ruby(raw_data))
            end
        end

        def raw_each
            return enum_for(__method__) unless block_given?
            return if stream.empty?

            @sample_count = 0
            @next_yield_time = nil
            @next_yield_index = nil

            min_index = self.min_index
            min_time  = self.min_time

            if min_time && stream.interval_lg.first > min_time
                min_time = nil
            elsif min_index || (min_time && !use_rt)
                stream.seek(min_index || min_time)
                stream.previous
            elsif min_time && use_rt
                return unless skip_to_realtime(min_time)

                stream.previous
            end

            stream.each_block(!(min_index || min_time)) do
                sample_index = stream.sample_index
                return self if max_index && max_index < sample_index
                return self if max_count && max_count <= @sample_count

                rt, lg = stream.time
                sample_time = use_rt ? rt : lg
                return self if max_time && max_time < sample_time

                if yield_sample?(sample_time, sample_index)
                    @sample_count += 1
                    data_block = stream.data_header
                    yield(data_block.rt, data_block.lg,
                          (stream.raw_data(data_block) if read_data))
                end
            end
            self
        end

        def skip_to_realtime(min_time)
            stream.each_block(true) do
                sample_index = stream.sample_index
                return if max_index && max_index < sample_index

                rt, = stream.time
                return true if rt >= min_time
            end
            false
        end

        # Yield the given sample if required by our configuration
        def yield_sample?(sample_time, sample_index)
            if every_time
                every_time = self.every_time.to_r
                @next_yield_time ||= sample_time
                while @next_yield_time <= sample_time
                    do_display = true
                    @next_yield_time += every_time
                end
                do_display
            elsif every_index
                @next_yield_index ||= sample_index
                if @next_yield_index <= sample_index
                    do_display = true
                    @next_yield_index += every_index
                end
                do_display
            else
                true
            end
        end
    end
end
