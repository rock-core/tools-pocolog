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

            it "raises ObsoleteIndexVersion if the index is of a version older than the library's" do
                File.open(logfile_path('idx'), 'w') do |io|
                    Format::Current.write_index(io, flexmock(stat: flexmock(size: 0, mtime: Time.now), path: nil), [], version: 0)
                end
                assert_raises(ObsoleteIndexVersion) do
                    File.open(logfile_path('idx')) do |io|
                        Format::Current.read_index_prologue(io)
                    end
                end
            end

            it "raises IndexVersion if the index is of a version newer than the library's" do
                File.open(logfile_path('idx'), 'w') do |io|
                    Format::Current.write_index(io, flexmock(stat: flexmock(size: 0, mtime: Time.now), path: nil), [], version: Format::Current::INDEX_VERSION + 1)
                end
                assert_raises(InvalidIndex) do
                    File.open(logfile_path('idx')) do |io|
                        Format::Current.read_index_prologue(io)
                    end
                end
            end

            describe "#read_index_stream_info" do
                it "raises InvalidIndex if the file is smaller than the expected file size" do
                    index_real_size = index_io.size
                    flexmock(index_io).should_receive(:size).and_return(index_real_size - 1)
                    e = assert_raises(InvalidIndex) do
                        Format::Current.read_index_stream_info(index_io)
                    end
                    assert_equal "index file should be of size #{index_real_size} but is of size #{index_io.size}", e.message
                end
                it "raises InvalidIndex if the file is bigger than the expected file size" do
                    index_real_size = index_io.size
                    flexmock(index_io).should_receive(:size).and_return(index_real_size + 1)
                    e = assert_raises(InvalidIndex) do
                        Format::Current.read_index_stream_info(index_io)
                    end
                    assert_equal "index file should be of size #{index_real_size} but is of size #{index_io.size}", e.message
                end
            end
        end
    end
end
