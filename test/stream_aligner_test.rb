require 'test_helper'

module Pocolog
    describe StreamAligner do
        attr_reader :logfile

        def open_logfile
            FileUtils.rm_f 'test.0.log'
            FileUtils.rm_f 'test.0.idx'
            @logfile = Pocolog::Logfiles.create('test')
        end

        def close_logfile
            @logfile.close
            @logfile = nil
        end

        def create_log_stream(name, data)
            double_t = Typelib::Registry.new.create_numeric '/double', 8, :float
            stream = logfile.create_stream(name, double_t)
            data.each do |v|
               stream.write(Time.at(v * 10), Time.at(v * 10), v)
            end
            stream
        end

        def create_aligner(*data)
            open_logfile
            data.each_with_index do |samples, i|
                create_log_stream("s#{i}", samples)
            end
            close_logfile

            logfile = Pocolog::Logfiles.open('test.0.log')
            streams = data.each_with_index.map do |_, i|
                logfile.stream("s#{i}")
            end
            aligner = StreamAligner.new(false, *streams)
            return aligner, *streams
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

        describe "#time" do
            it "returns nil on a rewound stream aligner" do
                aligner, _ = create_aligner [1, 2], [1.1, 1.2]
                aligner.seek(2)
                aligner.rewind
                assert_nil aligner.time
            end
            it "returns nil on an empty stream aligner" do
                aligner, _ = create_aligner [], []
                assert_nil aligner.time
            end
        end

        describe "#add_streams" do
            attr_reader :s0, :s1, :aligner
            before do
                open_logfile
                create_log_stream 's0', [1, 2, 3]
                create_log_stream 's1', [1.5, 2.5, 2.7, 3.2]
                close_logfile
                logfile = Pocolog::Logfiles.open('test.0.log')
                @s0 = logfile.stream('s0')
                @s1 = logfile.stream('s1')
                @aligner = StreamAligner.new(false)
                aligner.add_streams(s1)
            end

            it "picks base_time once and does not update it" do
                assert_equal 15_000_000, aligner.base_time
                aligner.add_streams(s0)
                assert_equal 15_000_000, aligner.base_time
            end

            it "aligns the new streams with the existing ones" do
                aligner.add_streams(s0)
                assert_equal [[1, 1], [0, 1.5], [1, 2], [0, 2.5], [0, 2.7], [1, 3], [0, 3.2]],
                    aligner.each.map { |stream_index, _, value| [stream_index, value] }
            end

            it "updates the size attribute" do
                assert_equal 4, aligner.size
                aligner.add_streams(s0)
                assert_equal 7, aligner.size
            end

            it "updates the time interval" do
                aligner.add_streams(s0)
                assert_equal [Time.at(10), Time.at(32)], aligner.interval_lg
            end

            it "updates the current position" do
                aligner.seek(1)
                assert_nil aligner.add_streams(s0)
                assert_equal 3, aligner.sample_index
            end

            describe "if the aligner is eof" do
                it "leaves it at past-the-end if the new streams do not add samples after the current sample set" do
                    aligner.seek(4)
                    assert_nil aligner.add_streams(s0)
                    assert aligner.eof?
                end

                it "sets the current position to the first new sample appended after the current end if the stream was eof?" do
                    aligner = StreamAligner.new(false)
                    aligner.add_streams(s0)
                    aligner.seek(3)
                    assert_equal [1, Time.at(32), 3.2], aligner.add_streams(s1)
                    assert_equal 6, aligner.sample_index
                end
            end

            it "updates the per-stream start positions" do
                assert_equal 0, aligner.global_pos_first_sample[0]
                aligner.add_streams(s0)
                assert_equal 1, aligner.global_pos_first_sample[0]
            end

            it "updates the per-stream start positions" do
                assert_equal 3, aligner.global_pos_last_sample[0]
                aligner.add_streams(s0)
                assert_equal 6, aligner.global_pos_last_sample[0]
            end
        end

        describe "#remove_streams" do
            attr_reader :s0, :s1, :aligner
            before do
                @aligner, @s0, @s1 = create_aligner [1, 2, 3], [1.5, 2.5, 2.7, 3.2]
            end

            it "does not update #base_time" do
                assert_equal 10_000_000, aligner.base_time
                aligner.remove_streams(s0)
                assert_equal 10_000_000, aligner.base_time
                aligner.remove_streams(s1)
                assert_equal 10_000_000, aligner.base_time
            end

            it "behaves when removing the last stream" do
                aligner.remove_streams(s1)
                aligner.remove_streams(s0)
                assert_equal 0, aligner.size
                assert_equal [], aligner.interval_lg
                assert aligner.global_pos_first_sample.empty?
                assert aligner.global_pos_last_sample.empty?
            end

            it "updates the alignment, and updates the stream indexes" do
                # We remove s0 to force the method to shift the index of the
                # remaining stream
                aligner.remove_streams(s0)
                assert_equal [[0, 1.5], [0, 2.5], [0, 2.7], [0, 3.2]],
                    aligner.each.map { |stream_index, _, value| [stream_index, value] }
            end

            it "updates the size attribute" do
                aligner.remove_streams(s1)
                assert_equal 3, aligner.size
            end

            it "updates the time interval" do
                aligner.remove_streams(s1)
                assert_equal [Time.at(10), Time.at(30)], aligner.interval_lg
            end

            it "leaves the position at past-the-end" do
                aligner.seek(7)
                assert_nil aligner.remove_streams(s0)
                assert aligner.eof?
            end

            it "updates the current position to the current sample if it still in the aligner and returns nil" do
                aligner.seek(1)
                assert_nil aligner.remove_streams(s0)
                assert_equal 0, aligner.sample_index
                assert_equal [s1, 0], aligner.sample_info(0)
            end

            it "updates the current position to the next still-present sample if it the current sample has been removed, and returns the new sample information" do
                aligner.seek(2)
                assert_equal [0, Time.at(25), 2.5], aligner.remove_streams(s0)
                assert_equal 1, aligner.sample_index
                assert_equal [s1, 1], aligner.sample_info(0)
            end

            it "behaves if the position-to-be-updated is the last sample" do
                aligner.seek(5)
                assert_equal [0, Time.at(32), 3.2], aligner.remove_streams(s0)
                assert_equal 3, aligner.sample_index
                assert_equal [s1, 3], aligner.sample_info(0)
            end

            it "updates the current position to past-the-end if the current sample and all the ones after it have been removed, and returns nil" do
                aligner.seek(6)
                assert_nil aligner.remove_streams(s1)
                assert aligner.eof?
            end

            it "updates the current position to before-the-beginning if all streams are removed" do
                aligner.seek(4)
                aligner.remove_streams(s1)
                aligner.remove_streams(s0)
                assert_equal(-1, aligner.sample_index)
            end

            it "updates the per-stream start positions" do
                assert_equal 1, aligner.global_pos_first_sample[1]
                aligner.remove_streams(s0)
                assert_equal 0, aligner.global_pos_first_sample[0]
                assert_equal 1, aligner.global_pos_first_sample.size
            end

            it "updates the per-stream start positions" do
                assert_equal 6, aligner.global_pos_last_sample[1]
                aligner.remove_streams(s0)
                assert_equal 3, aligner.global_pos_last_sample[0]
                assert_equal 1, aligner.global_pos_first_sample.size
            end
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
            it "raises if given an unknown stream" do
                aligner, s0, s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                open_logfile
                invalid_stream = create_log_stream "invalid_stream", []
                e = assert_raises(ArgumentError) do
                    aligner.first_sample_pos(invalid_stream)
                end
                assert_equal "invalid_stream (#{invalid_stream}) is not aligned in "\
                    "#{aligner}. Aligned streams are: s0, s1", e.message
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
            it "raises if given an unknown stream" do
                aligner, s0, s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                open_logfile
                invalid_stream = create_log_stream "invalid_stream", []
                e = assert_raises(ArgumentError) do
                    aligner.last_sample_pos(invalid_stream)
                end
                assert_equal "invalid_stream (#{invalid_stream}) is not aligned in "\
                    "#{aligner}. Aligned streams are: s0, s1", e.message
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

        describe "#pretty_print" do
            it "does not raise" do
                aligner, _ = create_aligner([1, 2, 3], [2.5, 3.5])
                PP.pp(aligner, "")
            end
        end

        describe "#seek_to_time" do
            it "raises RangeError on an empty stream" do
                aligner = StreamAligner.new(false)
                assert_raises(RangeError) { aligner.seek_to_time(Time.now) }
            end
            it "raises RangeError if the time is before the start of the streams" do
                aligner, _ = create_aligner [1, 2], [1.2]
                assert_raises(RangeError) { aligner.seek_to_time(Time.at(0)) }
            end
            it "raises RangeError if the time is after the end of the streams" do
                aligner, _ = create_aligner [1, 2], [1.2]
                assert_raises(RangeError) { aligner.seek_to_time(Time.at(310)) }
            end
            it "seeks to the first sample with the expected time if it exists" do
                aligner, _ = create_aligner [1], [1.2]
                assert_equal [0, Time.at(10), 1], aligner.seek_to_time(Time.at(10))
            end
            it "seeks to the first sample just after the expected time if none exists with the expected time" do
                aligner, _ = create_aligner [1], [1.2]
                assert_equal [1, Time.at(12), 1.2], aligner.seek_to_time(Time.at(11))
            end
            it "does not read the data if read_data is false" do
                aligner, _ = create_aligner [1], [1.2]
                assert_equal [1, Time.at(12)], aligner.seek_to_time(Time.at(11), false)
            end
        end

        describe "#seek_to_pos" do
            it "raises RangeError on an empty stream" do
                aligner = StreamAligner.new(false)
                assert_raises(RangeError) { aligner.seek_to_pos(0) }
            end
            it "raises RangeError if the position is negative" do
                aligner, _ = create_aligner [1, 2], [1.2]
                assert_raises(RangeError) { aligner.seek_to_pos(-1) }
            end
            it "raises RangeError if the position is after the end of the streams" do
                aligner, _ = create_aligner [1, 2], [1.2]
                assert_raises(RangeError) { aligner.seek_to_pos(5) }
            end
            it "returns to sample at the expected global index" do
                aligner, _ = create_aligner [1, 2], [1.2, 1.3]
                assert_equal [1, Time.at(13), 1.3], aligner.seek_to_pos(2)
            end
            it "does not read the data if read_data is false" do
                aligner, _ = create_aligner [1, 2], [1.2, 1.3]
                assert_equal [1, Time.at(13)], aligner.seek_to_pos(2, false)
            end
        end

        describe "#stream_indeX_for_name" do
            it "returns the index of the matching stream" do
                aligner, _s0, _s1 = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_equal 1, aligner.stream_index_for_name('s1')
            end
            it "returns nil if there is no match" do
                aligner, _ = create_aligner([1, 2, 3], [1.5, 3.5])
                assert_nil aligner.stream_index_for_name('does_not_exist')
            end
        end

        describe "#stream_index_for_type" do
            it "returns the index of the matching stream" do
                aligner, _s0 = create_aligner([1, 2, 3])
                assert_equal 0, aligner.stream_index_for_type('/double')
            end
            it "raises ArgumentError if there is more than one match" do
                aligner, _s0, _s1 = create_aligner([1, 2, 3], [3, 4])
                assert_raises(ArgumentError) do
                    aligner.stream_index_for_type('/double')
                end
            end

            it "returns nil if there is no match" do
                aligner, _ = create_aligner
                assert_nil aligner.stream_index_for_type('/does_not_exist')
            end
        end

        describe "#next" do
            it "returns realtime, logical time and the (stream_index, data) tuple as sample" do
                aligner, _s0, _s1 = create_aligner([1, 3], [2, 4])
                assert_equal [Time.at(10), Time.at(10), [0, 1]], aligner.next
                assert_equal [Time.at(20), Time.at(20), [1, 2]], aligner.next
            end
            it "returns nil at the end of stream" do
                aligner, _ = create_aligner
                assert !aligner.next
            end
        end
        describe "#previous" do
            it "returns realtime, logical time and the (stream_index, data) tuple as sample" do
                aligner, _s0, _s1 = create_aligner([1, 3], [2, 4])
                aligner.next
                aligner.next
                assert_equal [Time.at(10), Time.at(10), [0, 1]], aligner.previous
            end
            it "returns nil at the beginning of file" do
                aligner, _s0, _s1 = create_aligner([1, 3], [2, 4])
                assert_equal [Time.at(10), Time.at(10), [0, 1]], aligner.next
                assert_nil aligner.previous
            end
        end
    end
end


class TC_StreamAligner < Minitest::Test
    attr_reader :logfile
    attr_reader :stream

    def self.create_fixture
        registry = Typelib::Registry.new
        int_t    = registry.create_numeric '/int', 4, :sint
        logfile = Pocolog::Logfiles.create('test')
        all_values  = logfile.create_stream('all', int_t)
        odd_values  = logfile.create_stream('odd', int_t)
        even_values = logfile.create_stream('even', int_t)

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
        assert_equal [Time.at(0),Time.at(99,500)], stream.interval_lg
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

class TC_StreamAligner2 < Minitest::Test
    attr_reader :logfile
    attr_reader :stream
    attr_reader :expected_data

    def create_fixture
        logfile = Pocolog::Logfiles.create('test')
        int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
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
        index, time, data = stream.step
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
        @stream.export_to_file("export1")
        @stream.export_to_file("export2",30)
        @stream.export_to_file("export3",90,110)

        logfile2 = Pocolog::Logfiles.open('export1.0.log')
        stream2  = Pocolog::StreamAligner.new(false, logfile2.stream('all'), logfile2.stream('other'))
        assert_equal 200, stream2.size
        logfile2.close

        logfile2 = Pocolog::Logfiles.open('export2.0.log')
        stream2  = Pocolog::StreamAligner.new(false, logfile2.stream('all'), logfile2.stream('other'))
        assert_equal 170, stream2.size
        logfile2.close

        logfile2 = Pocolog::Logfiles.open('export3.0.log')
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

    ensure
        1.upto(3) do |i|
            FileUtils.rm_f "export#{i}.0.log"
            FileUtils.rm_f "export#{i}.0.idx"
        end
    end
end


