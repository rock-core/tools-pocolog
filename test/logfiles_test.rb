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
    end
end

