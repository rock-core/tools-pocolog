require 'pocolog'
require 'test/unit'

class TC_DataStream < Test::Unit::TestCase
    attr_reader :logfile
    attr_reader :stream
    attr_reader :expected_data

    def create_fixture
        logfile = Pocolog::Logfiles.create('test')
        all_values = logfile.stream('all', 'int', true)
        @expected_data = Array.new
        100.times do |i|
            all_values.write(Time.now, Time.now, i)
            expected_data << i
        end
        logfile.close
    end

    def setup
        create_fixture
        @logfile = Pocolog::Logfiles.open('test.0.log')
        @stream  = logfile.stream('all')
    end

    def teardown
        FileUtils.rm_f 'test.0.log'
        FileUtils.rm_f 'test.0.idx'
    end

    def test_properties
        assert !stream.eof?
        assert_equal 100, stream.size
    end

    def test_step_by_step_all_the_way
        2.times do
            stream_data  = Array.new
            sample_index = Array.new
            while (data = stream.next) && (stream_data.size <= expected_data.size * 2)
                stream_data << data[2]
                sample_index << stream.sample_index
            end
            # verify that calling #step again is harmless
            assert_equal 100, stream.sample_index
            assert !stream.next
            assert stream.eof?
            assert_equal expected_data, stream_data
            assert_equal expected_data, sample_index

            # Now go back
            stream_data = Array.new
            sample_index = Array.new
            while (data = stream.previous) && (stream_data.size <= expected_data.size * 2)
                stream_data << data[2]
                sample_index << stream.sample_index
            end
            # verify that calling #step again is harmless
            assert !stream.previous
            assert_equal expected_data, stream_data.reverse
            assert_equal expected_data, sample_index.reverse
        end
    end

    def test_rewind_previous
        stream.seek(10)
        stream.rewind
        assert !stream.previous
        assert_equal 0, stream.next[2]
    end

    def test_seek_index
        _, _, value = stream.seek(10)
        assert_equal 10, value
        assert_equal 10, stream.sample_index
        _, _, value = stream.next
        assert_equal 11, value
        assert_equal 11, stream.sample_index
        _, _, value = stream.previous
        assert_equal 10, value
        assert_equal 10, stream.sample_index

        _, _, value = stream.seek(20)
        assert_equal 20, value
        assert_equal 20, stream.sample_index
        _, _, value = stream.previous
        assert_equal 19, value
        assert_equal 19, stream.sample_index
        _, _, value = stream.next
        assert_equal 20, value
        assert_equal 20, stream.sample_index
    end

    def test_first
        stream.seek(10)
        assert_equal 0, stream.first[2]
        assert_equal 1, stream.next[2]
        assert_equal 0, stream.previous[2]

        stream.seek(10)
        assert_equal 0, stream.first[2]
        assert_equal nil, stream.previous
        assert_equal 0, stream.next[2]
    end

    def test_last
        stream.seek(10)
        assert_equal 99, stream.last[2]
        assert_equal 99, stream.previous[2]
        assert_equal nil, stream.next

        stream.seek(10)
        assert_equal 99, stream.last[2]
        assert_equal nil, stream.next
        assert_equal 99, stream.previous[2]
    end
end


