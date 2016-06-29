require 'test_helper'

module Pocolog
    describe Format do
        describe "index write and read" do
            attr_reader :base_time, :base_time_internal, :index_io
            before do
                @base_time = Time.at(3932414, 3241)
                @base_time_internal = StreamIndex.time_to_internal(base_time, 0)
                create_logfile 'test.0.log' do
                    stream = create_logfile_stream 'test.0.log'
                    write_logfile_sample base_time, base_time + 1, 1
                    write_logfile_sample base_time + 10, base_time + 20, 2
                end
                # Create the index
                Logfiles.open(logfile_path('test.0.log')).close
                @index_io = File.open(logfile_path('test.0.idx'))
            end
            after do
                @index_io.close if @index_io
            end

            it "saves the stream info" do
                info = Format::Current.read_index_stream_info(index_io)
                assert_equal 1, info.size

                stream_info = info.first
                assert_equal 2, stream_info.stream_size
                assert_equal [base_time_internal, base_time_internal + 10_000_000],
                    stream_info.interval_rt
                assert_equal [base_time_internal + 1_000_000, base_time_internal + 20_000_000],
                    stream_info.interval_lg
            end
        end
    end
end
