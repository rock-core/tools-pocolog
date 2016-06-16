require 'test_helper'

class TC_DataStream < Minitest::Test
    attr_reader :logfile
    attr_reader :stream
    attr_reader :expected_data

    def create_fixture
        registry = Typelib::Registry.new
        int_t = registry.create_numeric '/int', 4, :sint
        logfile = Pocolog::Logfiles.create('test')
        all_values = logfile.create_stream('all', int_t, 'test' => 'value', 'test2' => 'value2')
        @expected_data = Array.new
        100.times do |i|
            all_values.write(Time.at(i), Time.at(i * 100), i)
            expected_data << i
        end

        # Add a followup stream that fills in the file. It is used for a corner
        # case in #test_past_the_end_does_not_read_whole_file
        other_stream = logfile.create_stream('other', int_t)
        100.times do |i|
            other_stream.write(Time.at(i), Time.at(i * 100), i)
        end
        logfile.close
    end

    def setup
        create_fixture
        @file_size = File.stat('test.0.log').size
        @logfile = Pocolog::Logfiles.open('test.0.log')
        @stream  = logfile.stream('all')
    end

    def teardown
        FileUtils.rm_f 'test.0.log'
        FileUtils.rm_f 'test.0.idx'
    end

    def test_metadata
        assert_equal({'test' => "value", "test2" => "value2"}, stream.metadata)
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
                stream_data  << data[2]
                sample_index << stream.sample_index
            end
            # verify that calling #step again is harmless
            assert_equal 100, stream.sample_index
            assert_equal 100, stream.size
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

    def test_seek_time
        [0, 1].each do |offset|
            _, _, value = stream.seek(Time.at(1000, offset))
            assert_equal 10 + offset, value
            assert_equal 10 + offset, stream.sample_index
            _, _, value = stream.next
            assert_equal 11 + offset, value
            assert_equal 11 + offset, stream.sample_index
            _, _, value = stream.previous
            assert_equal 10 + offset, value
            assert_equal 10 + offset, stream.sample_index

            _, _, value = stream.seek(Time.at(2000, offset))
            assert_equal 20 + offset, value
            assert_equal 20 + offset, stream.sample_index
            _, _, value = stream.previous
            assert_equal 19 + offset, value
            assert_equal 19 + offset, stream.sample_index
            _, _, value = stream.next
            assert_equal 20 + offset, value
            assert_equal 20 + offset, stream.sample_index
        end
    end

    def test_get_data_using_data_header
        stream.seek(10)
        header = stream.data_header.dup
        stream.seek(20)
        assert_equal 10, stream.data(header)
        # Using #data(header) should not have moved the stream position
        assert_equal 21, stream.next[2]
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
        assert_equal 98, stream.previous[2]
        assert_equal 99, stream.next[2]

        stream.seek(10)
        assert_equal 99, stream.last[2]
        assert_equal nil, stream.next
        assert_equal 99, stream.previous[2]
    end

    def test_each_block
        data = []
        stream.each_block do
            data << [stream.sample_index, stream.data]
        end

        assert_equal (0...expected_data.size).to_a, data.map(&:first)
        assert_equal expected_data, data.map(&:last)
    end

    def test_samples?
        assert_equal(true,stream.samples?(0,100))
        assert_equal(false,stream.samples?(-10,-1))
        assert_equal(true,stream.samples?(-10,0))
        assert_equal(true,stream.samples?(10,90))
        assert_equal(true,stream.samples?(99,120))
        assert_equal(true,stream.samples?(Time.at(0),Time.at(99*100)))
        assert_equal(true,stream.samples?(Time.at(0),Time.at(200*100)))
        assert_equal(true,stream.samples?(Time.at(99*100),Time.at(200*100)))
        assert_equal(false,stream.samples?(Time.at(100*100),Time.at(200*100)))
    end

    def test_copy_to
        output = Pocolog::Logfiles.new(Typelib::Registry.new)
        output.new_file("copy_test.log")
        stream_output1 = output.create_stream("test1",stream.type)
        stream_output2 = output.create_stream("test2",stream.type)
        stream_output3 = output.create_stream("test3",stream.type)

        #copy samples according to the given intervals 
        stream.copy_to(0,120,stream_output1)
        stream.copy_to(20,30,stream_output2)
        stream.copy_to(Time.at(10*100),Time.at(30*100),stream_output3)
        output.close

        logfile = Pocolog::Logfiles.open('copy_test.log')
        assert_equal(3,logfile.streams.size)
        assert_equal(100,logfile.stream("test1").size)
        assert_equal(10,logfile.stream("test2").size)
        assert_equal(20,logfile.stream("test3").size)
        
        test3_stream = logfile.stream("test3")
        while(data = test3_stream.next)
            assert_equal expected_data[test3_stream.sample_index+10], data[2]
        end
        logfile.close
        FileUtils.rm_f "copy_test.log"
        FileUtils.rm_f "copy_test.idx"
    end
end

module Pocolog
    describe DataStream do
        describe "file sequences" do
            attr_reader :files
            attr_reader :base_time
            before do
                @base_time = Time.at(Time.now.tv_sec, Time.now.tv_usec)

                int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                file = Pocolog::Logfiles.create('file-sequence.0.log')
                stream = file.create_stream 'test', int_t
                stream.write base_time + 0, base_time + 10, 0
                stream.write base_time + 1, base_time + 11, 1
                file.close
                file = Pocolog::Logfiles.create('file-sequence.1.log')
                stream = file.create_stream 'test', int_t
                stream.write base_time + 2, base_time + 12, 2
                stream.write base_time + 3, base_time + 13, 3
                file.close

                ios = ['file-sequence.0.log', 'file-sequence.1.log'].map do |path|
                    File.open(path)
                end
                @files = Pocolog::Logfiles.new(*ios)
            end
            it "has a #size that is the sum of all the sizes" do
                stream = files.streams.first
                assert_equal 4, stream.size
            end
            it "transparently iterates through the files" do
                stream = files.streams.first
                assert_equal [base_time + 0, base_time + 10, 0], stream.next
                assert_equal 0, stream.sample_index
                assert_equal [base_time + 1, base_time + 11, 1], stream.next
                assert_equal 1, stream.sample_index
                assert_equal [base_time + 2, base_time + 12, 2], stream.next
                assert_equal 2, stream.sample_index
                assert_equal [base_time + 3, base_time + 13, 3], stream.next
                assert_equal 3, stream.sample_index
                assert_nil stream.next
            end
            it "seeks between the files" do
                stream = files.streams.first
                assert_equal 0, stream.read_one_data_sample(0)
                assert_equal 3, stream.read_one_data_sample(3)
                assert_equal 1, stream.read_one_data_sample(1)
            end
        end
    end
end
