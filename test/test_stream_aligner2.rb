require 'pocolog'
require 'test/unit'

class TC_StreamAligner2 < Test::Unit::TestCase
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
	assert_equal index, 0
	assert_equal data, expected_data[0]
	assert_equal time, Time.at(expected_data[0] * 100)
    end

    def test_full_replay
	cnt = 0
	while(!stream.eof?)
	    stream_index, time, data = stream.step()
	    if(cnt < 100)
		assert_equal stream_index, 0
	    else
		assert_equal stream_index, 1
	    end
	    assert_equal data, expected_data[cnt]
	    assert_equal time, Time.at(expected_data[cnt] * 100)
	    cnt = cnt + 1
	end
	
	assert_equal cnt, 200
    end

    def test_forward_backward
	cnt = 0
	while(!stream.eof?)
	    stream_index, time, data = stream.step()
	    if(cnt < 100)
		assert_equal stream_index, 0
	    else
		assert_equal stream_index, 1
	    end
	    assert_equal data, expected_data[cnt]
	    assert_equal time, Time.at(expected_data[cnt] * 100)
	    cnt = cnt + 1
	end
	
	assert_equal cnt, 200
	cnt = cnt - 1
	
	while(cnt > 1)
	    cnt = cnt - 1
	    stream_index, time, data = stream.step_back()
	    if(cnt < 100)
		assert_equal stream_index, 0
	    else
		assert_equal stream_index, 1
	    end
	    assert_equal data, expected_data[cnt]
	    assert_equal time, Time.at(expected_data[cnt] * 100)
	end	    
	
    end

    def test_export_to_file
        @stream.export_to_file("export1.log")
        @stream.export_to_file("export2.log",30)
        @stream.export_to_file("export3.log",90,109)

        logfile2 = Pocolog::Logfiles.open('export1.log')
        stream2  = Pocolog::StreamAligner.new(false, logfile2.stream('all'), logfile2.stream('other'))
        assert_equal 200, stream2.size
        logfile2.close

        logfile2 = Pocolog::Logfiles.open('export2.log')
        stream2  = Pocolog::StreamAligner.new(false, logfile2.stream('all'), logfile2.stream('other'))
        assert_equal 170, stream2.size
        logfile2.close

        logfile2 = Pocolog::Logfiles.open('export3.log')
        stream2  = Pocolog::StreamAligner.new(false, logfile2.stream('all'), logfile2.stream('other'))
        assert_equal 20, stream2.size

        cnt = 0
	while(!stream2.eof?)
	    stream_index, time, data = stream2.step()
	    if(cnt < 10)
		assert_equal stream_index, 0
	    else
		assert_equal stream_index, 1
	    end
	    assert_equal data, expected_data[cnt+90]
	    assert_equal time, Time.at(expected_data[cnt+90] * 100)
	    cnt = cnt + 1
	end
        logfile2.close

        1.upto(3) do |i|
            FileUtils.rm_f "export#{i}.log"
            FileUtils.rm_f "export#{i}.idx"
        end
    end
end
