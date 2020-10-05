require 'pocolog'
require 'benchmark'

file = Pocolog::Logfiles.open ARGV.first
Pocolog.logger.level = Logger::WARN

Benchmark.bm(20) do |x|
    x.report "incremental" do
        aligner = Pocolog::StreamAligner.new(false)
        file.streams.each do |s|
            aligner.add_streams(s)
        end
    end
    x.report "one-time" do
        aligner = Pocolog::StreamAligner.new(false)
        aligner.add_streams(*file.streams)
    end
end

