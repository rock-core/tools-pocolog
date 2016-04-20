#! /usr/bin/env ruby

require 'pocolog'
file = Pocolog::Logfiles.create 'fixture'
int_t = file.registry.create_numeric '/int_t', 4, :sint

stream_count = Integer(ARGV.shift)
stream_min_size = Integer(ARGV.shift)
stream_max_size = Integer(ARGV.shift)

total_size = 0
stream_count.times do |i|
    stream_size = stream_min_size + rand(stream_max_size - stream_min_size)
    sample_step = stream_max_size * 1_000_000 / stream_size
    stream = file.create_stream i.to_s, int_t

    time = Time.now
    stream_size.times do |i|
        stream.write time, time, i
        time += (sample_step / 2 + rand(sample_step)) * 0.000001
    end

    STDERR.puts "created stream #{i} with #{stream_size} samples separated on average by #{Float(sample_step) / 1_000_000}s"
    total_size += stream_size
end
STDERR.puts "#{total_size} samples total"

