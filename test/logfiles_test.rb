require 'test_helper'

module Pocolog
    describe Logfiles do
        attr_reader :stream_all_samples
        before do
            @stream_all_samples = Array.new
            create_logfile 'test.0.log' do
                create_logfile_stream 'all'
                100.times do |i|
                    stream_all_samples << [Time.at(i), Time.at(i * 100), i]
                    write_logfile_sample Time.at(i), Time.at(i * 100), i
                end
            end
        end

        describe "#has_stream?" do
            attr_reader :logfile
            before do
                @logfile = open_logfile 'test.0.log'
            end
            it "returns true for a stream with that name" do
                assert(logfile.has_stream?('all'))
            end
            it "returns false for a non existent stream" do
                refute logfile.has_stream?('does_not_exist')
            end
        end

        describe "#initialize" do
            it "builds the index if one does not exist" do
                flexmock(Pocolog::Logfiles).new_instances.
                    should_receive(:rebuild_and_load_index).once.pass_thru
                open_logfile 'test.0.log'
                assert File.exist?(logfile_path('test.0.idx'))
                # Verify that the index is valid by reading it
                File.open(logfile_path('test.0.idx')) do |io|
                    Format::Current.read_index(io)
                end
            end
            it "does not rebuild an index if one exists" do
                open_logfile 'test.0.log'
                flexmock(Pocolog::Logfiles).new_instances.
                    should_receive(:rebuild_and_load_index).never
                logfile = open_logfile 'test.0.log'
                # Check that the loaded index is valid
                assert_equal stream_all_samples, logfile.stream('all').samples.to_a
            end
        end
    end
end

