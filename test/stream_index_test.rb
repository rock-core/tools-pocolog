require 'test_helper'

module Pocolog
    describe StreamIndex do
        attr_reader :stream_index
        before do
            @stream_index = StreamIndex.new
        end

        def assert_time_equal(expected, actual)
            assert_equal expected.tv_sec, actual.tv_sec
            assert_equal expected.tv_usec, actual.tv_usec
        end

        describe "#base_time" do
            it "sets the base time to the given value" do
                stream_index.add_sample(0, base_time = Time.now)
                stream_index.base_time = base_time - 1
                assert_equal StreamIndex.time_to_internal(base_time - 1, 0), stream_index.base_time
            end
            it "offsets the internal time representation if needed" do
                stream_index.add_sample(0, base_time = Time.now)
                stream_index.base_time = base_time - 1
                assert_equal 1_000_000, stream_index.internal_time_by_sample_number(0)
            end
        end

        describe "#size" do
            it "returns the number of samples in the index" do
                assert_equal 0, stream_index.size
                stream_index.add_sample(0, base_time = Time.now)
                assert_equal 1, stream_index.size
            end
        end

        describe "#add_sample" do
            it "appends the sample to the index" do
                stream_index.add_sample(0, base_time = Time.now)
                assert_time_equal base_time, stream_index.time_by_sample_number(0)
            end
            it "sets the base time if it not already set" do
                stream_index.add_sample(0, base_time = Time.now)
                assert_equal StreamIndex.time_to_internal(base_time, 0), stream_index.base_time
            end
            it "does not reset the base time if it has already been set" do
                stream_index.add_sample(0, base_time = Time.now)
                stream_index.add_sample(0, base_time + 1)
                assert_equal StreamIndex.time_to_internal(base_time, 0), stream_index.base_time
                assert_equal 1_000_000, stream_index.internal_time_by_sample_number(1)
            end
        end

        describe "#sample_number_by_time" do
            attr_reader :base_time
            before do
                @base_time = Time.now
                stream_index.add_sample(0, base_time)
                stream_index.add_sample(1, base_time + 1)
                stream_index.add_sample(2, base_time + 2)
            end

            it "returns the earliest sample whose time is not before the given time" do
                assert_equal 1, stream_index.sample_number_by_time(base_time + 0.5)
            end
            it "returns 0 if the time is before the first sample" do
                assert_equal 0, stream_index.sample_number_by_time(base_time - 0.5)
            end
            it "returns size if the time is after the last sample" do
                assert_equal 3, stream_index.sample_number_by_time(base_time + 3)
            end
        end

        describe "#file_position_by_sample_number" do
            before do
                stream_index.add_sample(0, base_time = Time.now)
                stream_index.add_sample(1, base_time + 1)
                stream_index.add_sample(2, base_time + 2)
            end
            it "returns the sample's file position" do
                assert_equal 1, stream_index.file_position_by_sample_number(1)
            end
            it "raises IndexError if the sample number is out of bounds" do
                assert_raises(IndexError) do
                    stream_index.file_position_by_sample_number(5)
                end
                assert_raises(IndexError) do
                    stream_index.file_position_by_sample_number(-5)
                end
            end
        end

        describe "#concat" do
            attr_reader :new_index, :base_time
            before do
                @base_time = Time.now
                stream_index.add_sample(0, base_time)
                stream_index.add_sample(1, base_time + 1)
                stream_index.add_sample(2, base_time + 2)
                @new_index = StreamIndex.new
                new_index.add_sample(3, base_time + 4)
                new_index.add_sample(4, base_time + 5)
            end
            it "adds the new index' entries to the existing" do
                stream_index.concat(new_index)
                assert_equal 5, stream_index.size
                assert_equal 3, stream_index.file_position_by_sample_number(3)
                assert_time_equal base_time + 4, stream_index.time_by_sample_number(3)
            end
            it "shifts the index so that find-by-time works for the new entries" do
                stream_index.concat(new_index)
                assert_equal 3, stream_index.sample_number_by_time(base_time + 2.5)
            end
            it "uses the receiver's base time to encode the new entries" do
                new_index.base_time = base_time - 1
                stream_index.concat(new_index)
                assert_equal StreamIndex.time_to_internal(base_time, 0), stream_index.base_time
                assert_equal 4_000_000, stream_index.internal_time_by_sample_number(3)
            end
            it "optionally applies an offset on the file position" do
                stream_index.concat(new_index, 10)
                assert_equal 13, stream_index.file_position_by_sample_number(3)
            end
        end
    end
end

