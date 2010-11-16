require 'pocolog'
require 'test/unit'

Pocolog::Logfiles::StreamInfo::INDEX_STEP = 2 

class TC_StreamAligner < Test::Unit::TestCase
    attr_reader :logfile
    attr_reader :stream

    def create_fixture
        logfile = Pocolog::Logfiles.create('test')
        all_values  = logfile.stream('all', 'int', true)
        odd_values  = logfile.stream('odd', 'int', true)
        even_values = logfile.stream('even', 'int', true)

        @interleaved_data = []
        100.times do |i|
            all_values.write(Time.at(i * 1000), Time.at(i), i)
            interleaved_data << i
            if i % 2 == 0
                even_values.write(Time.at(i * 1000, 500), Time.at(i, 500), i * 100)
                interleaved_data << i * 100
            else
                odd_values.write(Time.at(i * 1000, 500), Time.at(i, 500), i * 10000)
                interleaved_data << i * 10000
            end
        end
        logfile.close
    end

    # The data stream as the StreamAligner should give to us
    attr_reader :interleaved_data
    def setup
        create_fixture
        @logfile = Pocolog::Logfiles.open('test.0.log')
        @stream  = Pocolog::StreamAligner.new(false, logfile.stream('all'), logfile.stream('odd'), logfile.stream('even'))
    end

    def teardown
        FileUtils.rm_f 'test.0.log'
        FileUtils.rm_f 'test.0.idx'
    end

    def test_properties
        assert !stream.eof?
        assert_equal 200, stream.size
    end

    def test_playing_forward_backward
        assert stream.playing_forward?
        stream.next
        assert stream.playing_forward?
        stream.previous
        assert stream.playing_backward?
        stream.next
        assert stream.playing_forward?
    end

    def test_step_by_step_all_the_way
        2.times do
            stream_indexes, sample_indexes, all_data = Array.new, Array.new, Array.new
            while (data = stream.step) && (all_data.size <= interleaved_data.size * 2)
                stream_indexes  << data[0]
                sample_indexes << stream.sample_index
                all_data << data[2]
            end
            # verify that calling #step again is harmless
            assert !stream.step 
            assert stream.eof?
            assert_equal 200, stream.sample_index
            assert_equal interleaved_data, all_data
            assert_equal [[0, 2, 0, 1]].to_set, stream_indexes.each_slice(4).to_set
            assert_equal (0...200).to_a, sample_indexes

            # Now go back
            stream_indexes, sample_indexes, all_data = Array.new, Array.new, Array.new
            while (data = stream.step_back) && (all_data.size <= interleaved_data.size * 2)
                stream_indexes  << data[0]
                sample_indexes << stream.sample_index
                all_data << data[2]
            end
            assert_equal interleaved_data, all_data.reverse
            assert_equal [[0, 2, 0, 1]].to_set, stream_indexes.reverse.each_slice(4).to_set
            assert_equal (0...200).to_a, sample_indexes.reverse
        end
    end

    # Checks that stepping and stepping back in the middle of the stream works
    # fine
    def test_step_by_step_middle
        assert_equal [0, Time.at(0), 0], stream.step
        assert_equal [2, Time.at(0, 500), 0], stream.step
        assert_equal [0, Time.at(1), 1], stream.step
        assert_equal [1, Time.at(1, 500), 10000], stream.step
        assert_equal [0, Time.at(1), 1], stream.step_back
        assert_equal [2, Time.at(0, 500), 0], stream.step_back
        assert_equal [0, Time.at(1), 1], stream.step
        assert_equal [1, Time.at(1, 500), 10000], stream.step
    end

    # Tests seeking on an integer position
    def test_seek_at_position
        sample = stream.seek(10)
        assert_equal 10, stream.sample_index
        assert_equal Time.at(5), stream.time
        assert_equal [0, Time.at(5), 5], sample

        # Check that seeking did not break step / step_back
        assert_equal [1, Time.at(5, 500), 50000], stream.step
        assert_equal [0, Time.at(6), 6], stream.step
        assert_equal [2, Time.at(6, 500), 600], stream.step
        sample = stream.seek(10)
        assert_equal [2, Time.at(4, 500), 400], stream.step_back
        assert_equal [0, Time.at(4), 4], stream.step_back
        assert_equal [1, Time.at(3, 500), 30000], stream.step_back
    end
end

