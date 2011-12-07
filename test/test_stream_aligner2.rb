require 'pocolog'
require 'test/unit'

class TC_DataStream < Test::Unit::TestCase
    attr_reader :logfile
    attr_reader :stream
    attr_reader :expected_data

    def create_fixture
        logfile = Pocolog::Logfiles.create('test')
        all_values = logfile.create_stream('all', 'int', 'test' => 'value', 'test2' => 'value2')
        @expected_data = Array.new
        100.times do |i|
            all_values.write(Time.at(i), Time.at(i * 100), i)
            expected_data << i
        end

        # Add a followup stream that fills in the file. It is used for a corner
        # case in #test_past_the_end_does_not_read_whole_file
        other_stream = logfile.create_stream('other', 'int')
        100.times do |i|
	    i = i + 100
            other_stream.write(Time.at(i), Time.at(i * 100), i)
            expected_data << i
        end
        logfile.close
    end
    
    attr_reader :interleaved_data
    def setup
        create_fixture
        @logfile = Pocolog::Logfiles.open('test.0.log')
        @stream  = Pocolog::StreamAligner.new(false, logfile.stream('all'), logfile.stream('other'))
    end
    
    def teardown
        FileUtils.rm_f 'test.0.log'
        FileUtils.rm_f 'test.0.idx'
    end

    def test_properties
        assert !stream.eof?
        assert_equal 200, stream.size
    end


    def test_start_of_stream
	index, time, data = stream.step()
	assert index, 0
	assert data, expected_data[0]
	assert time, Time.at(expected_data[0])
    end

    def test_full_replay
	cnt = 0
	while(!stream.eof?)
	    index, time, data = stream.step()
	    assert index, cnt
	    assert data, expected_data[cnt]
	    assert time, Time.at(expected_data[cnt])
	    cnt = cnt + 1
	end
	
	assert cnt, 200
    end

    def test_forward_backward
	cnt = 0
	while(!stream.eof?)
	    index, time, data = stream.step()
	    assert index, cnt
	    assert data, expected_data[cnt]
	    assert time, Time.at(expected_data[cnt])
	    cnt = cnt + 1
	end
	
	assert cnt, 200
	
	while(cnt > 1)
	    cnt = cnt - 1
	    index, time, data = stream.step_back()
	    assert index, cnt
	    assert data, expected_data[cnt]
	    assert time, Time.at(expected_data[cnt])
	end	    
	
    end

end
