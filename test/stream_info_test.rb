require 'test_helper'

module Pocolog
    describe StreamInfo do
        attr_reader :stream_info
        attr_reader :base_time

        before do
            @stream_info = StreamInfo.new
            @base_time = Time.now
        end

        describe "#add_sample" do
            it "initializes the various intervals on first call" do
                stream_info.add_sample(0, base_time, base_time + 1)
                assert_equal [0, 0], stream_info.interval_io
                assert_equal [base_time, base_time], stream_info.interval_rt
                assert_equal [base_time + 1, base_time + 1], stream_info.interval_lg
            end
            it "updates the upper bound of the intervals" do
                stream_info.add_sample(0, base_time, base_time + 1)
                stream_info.add_sample(1, base_time + 1, base_time + 2)
                assert_equal [0, 1], stream_info.interval_io
                assert_equal [base_time, base_time + 1], stream_info.interval_rt
                assert_equal [base_time + 1, base_time + 2], stream_info.interval_lg
            end
            it "updates the size" do
                stream_info.add_sample(0, base_time, base_time + 1)
                stream_info.add_sample(1, base_time, base_time + 1)
                assert_equal 2, stream_info.size
            end
            it "adds the sample to the underlying index with the logical time" do
                flexmock(stream_info.index).should_receive(:add_sample).once.
                    with(0, base_time + 1)
                stream_info.add_sample(0, base_time, base_time + 1)
            end
            it "raises ArgumentError if attempting to add a new sample whose position is lower or equal than the current upper bound" do
                stream_info.add_sample(0, base_time, base_time + 1)
                assert_raises(ArgumentError) do
                    stream_info.add_sample(0, base_time + 1, base_time + 2)
                end
                assert_raises(ArgumentError) do
                    stream_info.add_sample(-1, base_time + 1, base_time + 2)
                end
            end
            it "raises ArgumentError if attempting to add a new sample whose realtime is lower than the current upper bound" do
                stream_info.add_sample(0, base_time, base_time + 1)
                assert_raises(ArgumentError) do
                    stream_info.add_sample(1, base_time - 1, base_time + 2)
                end
            end
            it "raises ArgumentError if attempting to add a new sample whose logical time is lower than the current upper bound" do
                stream_info.add_sample(0, base_time, base_time + 1)
                assert_raises(ArgumentError) do
                    stream_info.add_sample(1, base_time + 1, base_time)
                end
            end
        end

        describe "#concat" do
            attr_reader :new_stream_info
            before do
                stream_info.add_sample(0, base_time, base_time + 1)
                stream_info.add_sample(1, base_time + 1, base_time + 2)
                @new_stream_info = StreamInfo.new
            end
            it "updates the intervals" do
                new_stream_info.add_sample(2, base_time + 2, base_time + 3)
                stream_info.concat(new_stream_info)
                assert_equal [0, 2], stream_info.interval_io
                assert_equal [base_time, base_time + 2], stream_info.interval_rt
                assert_equal [base_time + 1, base_time + 3], stream_info.interval_lg
            end
            it "updates the size" do
                new_stream_info.add_sample(2, base_time + 2, base_time + 3)
                stream_info.concat(new_stream_info)
                assert_equal 3, stream_info.size
            end
            it "can concatenate with an empty receiver" do
                empty_stream = StreamInfo.new
                new_stream_info.add_sample(2, base_time + 2, base_time + 3)
                empty_stream.concat(new_stream_info)
                assert_equal new_stream_info.interval_io, empty_stream.interval_io
                assert_equal new_stream_info.interval_lg, empty_stream.interval_lg
                assert_equal new_stream_info.interval_rt, empty_stream.interval_rt
                assert_equal 1, empty_stream.size
                assert_equal empty_stream.index.index_map, new_stream_info.index.index_map
            end
            it "can concatenate with an empty argument" do
                empty_stream = StreamInfo.new
                stream_info.concat(empty_stream)
                assert_equal [0, 1], stream_info.interval_io
                assert_equal [base_time, base_time + 1], stream_info.interval_rt
                assert_equal [base_time + 1, base_time + 2], stream_info.interval_lg
                assert_equal 2, stream_info.size
            end
            it "concatenates the indexes" do
                new_stream_info.add_sample(2, base_time + 2, base_time + 3)
                flexmock(stream_info.index).should_receive(:concat).once.
                    with(new_stream_info.index, 10)
                stream_info.concat(new_stream_info, 10)
            end
            it "raises ArgumentError if the argument's file positions do not stricly follow self" do
                new_stream_info.add_sample(1, base_time + 2, base_time + 3)
                assert_raises(ArgumentError) do
                    stream_info.concat(new_stream_info)
                end
            end
            it "raises ArgumentError if the argument's realtime range do not follow self" do
                new_stream_info.add_sample(2, base_time, base_time + 3)
                assert_raises(ArgumentError) do
                    stream_info.concat(new_stream_info)
                end
            end
            it "raises ArgumentError if the argument's logical time range do not follow self" do
                new_stream_info.add_sample(2, base_time + 2, base_time + 1)
                assert_raises(ArgumentError) do
                    stream_info.concat(new_stream_info)
                end
            end
        end
    end
end

