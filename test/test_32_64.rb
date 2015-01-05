require 'minitest/autorun'
require 'pocolog'

class TC_3264 < Minitest::Test
    DATA_PATH  = File.expand_path("data", File.dirname(__FILE__))
    LOGS_32BIT = %w{camera32bit.0.log}

    def test_read_32bit
        LOGS_32BIT.each do |filename, streams|
            logfile = Pocolog::Logfiles.open(File.join(DATA_PATH, filename))
            logfile.streams.each do |s|
                s.samples.to_a
            end
        end
    end
end

