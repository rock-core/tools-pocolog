require 'priority_queue'
require 'rbtree'

REPEAT_COUNT = 100
STREAM_COUNTS = [1, 5, 10]
SAMPLE_COUNT = [10000, 10000]

class Stream
    attr_reader :index
    attr_reader :times
    attr_reader :current_pointer

    def current
        times[current_pointer]
    end
    def next
        times[@current_pointer += 1]
    end
    def rewind
        @current_pointer = 0
    end

    def initialize(index, sample_count)
        @index = index
        @current_pointer = 0

        current = 0
        @times = Array.new
        sample_count.times do |i|
            times << (current += rand(1))
        end
    end
    def <=>(other)
        current <=> other.current
    end
end

class PQAdaptor
    def initialize; @pq = PriorityQueue.new end
    def push(stream)
        @pq.push(stream, stream.current)
    end
    def empty?
        @pq.empty?
    end
    def next
        min = @pq.min_key
        if min.next
            @pq.change_priority(min, min.current)
        else
            @pq.delete_min
        end
        min
    end
end

class DumbPQ
    def initialize; @elements = Array.new end
    def push(stream)
        @elements << stream
    end
    def empty?
        @elements.empty?
    end
    def next
        v = @elements.min_by { |v| v.current }
        if !v.next
            @elements.delete(v)
        end
        v
    end
end

class RBTreePQ
    def initialize; @elements = RBTree.new end
    def push(stream)
        @elements[stream] = stream
    end
    def empty?
        @elements.empty?
    end
    def next
        v, _ = @elements.shift
        if v.next
            @elements[v] = v
        end
        v
    end
end

PQ_IMPLEMENTATIONS = [RBTreePQ, PQAdaptor, DumbPQ]

STDOUT.sync = true
STREAM_COUNTS.each do |stream_count|
    streams = (0...stream_count).map { Stream.new(stream_count, SAMPLE_COUNT[0] + rand(SAMPLE_COUNT[1])) }
    PQ_IMPLEMENTATIONS.each do |pq_class|
        GC.start
        puts "starting #{stream_count} #{pq_class}"
        samples = []
        REPEAT_COUNT.times do |i|
            streams.each(&:rewind)
            print "\r#{i}/#{REPEAT_COUNT}"
            pq = pq_class.new

            # Initialize
            streams.each do |s|
                pq.push(s)
            end

            start = Time.now
            last_value = nil
            while !pq.empty?
                new_value = pq.next
                last_value ||= new_value
                if (last_value <=> new_value) == -1
                    raise
                end
            end
            end_time = Time.now

            samples << (end_time - start)
        end
        mean = samples.inject(0, &:+) / samples.size
        std  = Math.sqrt(samples.inject(0) { |sum, v| sum + (v - mean) ** 2 } / samples.size)

        puts "\r#{pq_class} #{stream_count}: #{mean} #{std}"
    end
end

