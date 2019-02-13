require 'pocolog/test'

class TC_StreamAligner < Minitest::Test
    attr_reader :logfile
    attr_reader :stream

    def self.create_fixture
        logfile = Pocolog::Logfiles.create('test')
        all_values  = logfile.stream('all', 'int', true)
        odd_values  = logfile.stream('odd', 'int', true)
        even_values = logfile.stream('even', 'int', true)

        interleaved_data = []
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
        interleaved_data
    end

    # The data stream as the StreamAligner should give to us
    attr_reader :interleaved_data
    def setup
        @interleaved_data = TC_StreamAligner.create_fixture
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
        assert_equal [Time.at(0),Time.at(99,500)], stream.time_interval
    end

    def test_eof_is_a_valid_loop_termination_condition
        while !@stream.eof?
            index = stream.step
            assert index, "failed at #{stream.sample_index}"
        end
    end

    def test_time_advancing
        cnt = 0
        last_time = nil
        while !@stream.eof?
            index, time, data = stream.step
            assert(!last_time || last_time < time)
            last_time = time
            cnt = cnt + 1
        end
        last_time = nil
        stream.step
        while cnt > 0
            index, time, data = stream.step_back()
            assert(!last_time || last_time > time)
            cnt = cnt - 1
            last_time = time
        end     
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

        #check if seeking is working if index is not cached 
        #see INDEX_STEP
        sample = stream.seek(21)
        assert_equal 21, stream.sample_index
        assert_equal Time.at(10,500), stream.time
        assert_equal [2, Time.at(10,500), 1000], sample
    end

    def test_step_to_eof
        stream.seek(197)
        assert_equal false, stream.eof?
        stream.step
        assert_equal false, stream.eof?
        stream.step
        assert_equal true, stream.eof?
    end

    # Tests seeking on a time position
    def test_seek_at_time
        #no sample must have a later logical time
        #seek returns the last sample possible
        sample = stream.seek(Time.at(5))
        assert_equal 10, stream.sample_index
        assert_equal Time.at(5), stream.time
        assert_equal [0, Time.at(5), 5], sample

        # Check that seeking did not break step / step_back
        assert_equal [1, Time.at(5,500), 50000], stream.step
        assert_equal [0, Time.at(6), 6], stream.step
        assert_equal [2, Time.at(6, 500), 600], stream.step
        sample = stream.seek(Time.at(50))
        assert_equal [1, Time.at(49, 500), 490000], stream.step_back
        assert_equal [0, Time.at(49), 49], stream.step_back
        assert_equal [2, Time.at(48, 500), 4800], stream.step_back
    end
end

module Pocolog
    describe StreamAligner do
        def create_aligner(s0_data, s1_data)
            FileUtils.rm_f 'test.0.log'
            FileUtils.rm_f 'test.0.idx'

            logfile = Pocolog::Logfiles.create('test')
            s0 = logfile.stream('s0', 'double', true)
            s0_data.each do |v|
                s0.write(Time.at(v * 10), Time.at(v * 10), v)
            end
            s1 = logfile.stream('s1', 'double', true)
            s1_data.each do |v|
                s1.write(Time.at(v * 10), Time.at(v * 10), v)
            end
            logfile.close

            logfile = Pocolog::Logfiles.open('test.0.log')
            s0 = logfile.stream('s0')
            s1 = logfile.stream('s1')
            aligner = StreamAligner.new(false, s0, s1)
            return aligner, s0, s1
        end

        after do
            FileUtils.rm_f 'test.0.log'
            FileUtils.rm_f 'test.0.idx'
            FileUtils.rm_f 'export.0.log'
            FileUtils.rm_f 'export.0.idx'
        end

        it "sorts samples in a stable way" do
            aligner, _ = create_aligner([1, 2, 3], [2, 3])
            assert_equal [0, Time.at(2*10)], aligner.seek_to_pos(1, false)
            assert_equal [1, Time.at(2*10)], aligner.seek_to_pos(2, false)
            assert_equal [0, Time.at(3*10)], aligner.seek_to_pos(3, false)
            assert_equal [1, Time.at(3*10)], aligner.seek_to_pos(4, false)
        end

        describe "#find_first_stream_sample_after" do

            it "returns the stream-local position of the next sample if the global positition points to a sample of the expected stream" do
                aligner, s0, _s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 3, aligner.find_first_stream_sample_after(3, s0)
            end
            it "returns the stream-local position of the next sample after the global positition, if the global position points to a sample not of the expected stream" do
                aligner, _s0, s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 1, aligner.find_first_stream_sample_after(2, s1)
            end
            it "deals with multiple samples having the same time by returning the sample strictly after the global position" do
                aligner, s0, s1 = create_aligner([1, 2, 3], [3])
                assert_equal 0, aligner.find_first_stream_sample_after(2, s1)
                aligner, _s0, s1 = create_aligner([1, 2, 3], [3])
                assert_equal 3, aligner.find_first_stream_sample_after(2, s0)
                aligner, _s0, s1 = create_aligner([3], [1, 2, 3])
                assert_equal 2, aligner.find_first_stream_sample_after(2, s1)
            end
            it "returns past-the-end if there is no sample in the expected stream after the given position" do
                aligner, s0, _s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 3, aligner.find_first_stream_sample_after(4, s0)
            end
        end

        describe "#first_sample_pos" do
            it "returns the global index of the first sample of the given stream index" do
                aligner, _s0, _s1 = create_aligner([1, 2, 3], [2.5, 3.5])
                assert_equal 0, aligner.first_sample_pos(0)
                assert_equal 2, aligner.first_sample_pos(1)
            end
            it "returns the global index of the first sample of the given stream" do
                aligner, s0, s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 0, aligner.first_sample_pos(s0)
                assert_equal 1, aligner.first_sample_pos(s1)
            end
        end

        describe "#last_sample_pos" do
            it "returns the global index of the last sample of the given stream index" do
                aligner, _s0, _s1 = create_aligner([1, 2, 3], [2.5, 3.5])
                assert_equal 3, aligner.last_sample_pos(0)
                assert_equal 4, aligner.last_sample_pos(1)
            end
            it "returns the global index of the last sample of the given stream" do
                aligner, s0, s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 3, aligner.last_sample_pos(s0)
                assert_equal 4, aligner.last_sample_pos(s1)
            end
        end

        describe "#size" do
            it "returns the total number of samples in the aligner" do
                aligner, _ = create_aligner([1, 2, 3], [2.5, 3.5])
                assert_equal 5, aligner.size
            end
        end

        describe "#export_to_file" do
            it "exports all the aligned stream if given no arguments" do
                aligner, _ = create_aligner([1, 2, 3], [2.5, 3.5])
                aligner.export_to_file('export')
                new_aligner = Logfiles.open('export.0.log').stream_aligner
                assert_equal aligner.each.to_a, new_aligner.each.to_a
            end

            it "allows to override the start position" do
                aligner, _ = create_aligner([1, 2, 3], [2.5, 3.5])
                aligner.export_to_file('export', 1)
                new_aligner = Logfiles.open('export.0.log').stream_aligner
                assert_equal aligner.each.to_a[1..-1], new_aligner.each.to_a
            end

            it "allows to override the end position" do
                aligner, _ = create_aligner([1, 2, 3], [2.5, 3.5])
                aligner.export_to_file('export', 1, 4)
                new_aligner = Logfiles.open('export.0.log').stream_aligner
                assert_equal aligner.each.to_a[1..-2], new_aligner.each.to_a
            end
        end
    end
end

