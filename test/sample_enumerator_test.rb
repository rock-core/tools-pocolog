require 'test_helper'

module Pocolog
    describe SampleEnumerator do
        attr_reader :now

        before do
            @now = Time.at(Time.now.tv_sec, Time.now.tv_usec)
            @data = [
                [now - 1/10r, now, 0],
                [now + 11/10r, now + 1, 1],
                [now + 19/10r, now + 2, 2],
                [now + 31/10r, now + 3, 3]
            ]

            @logfile_path = create_logfile "test.0.log" do
                create_logfile_stream "test"
                @data.each do |rt, lg, sample|
                    write_logfile_sample rt, lg, sample
                end
            end

            @samples = open_logfile_stream("test.0.log", "test").samples
        end

        it "enumerates the raw samples" do
            assert_equal @data,
                         @samples.raw_each.map { |rt, lg, data| [rt, lg, data.to_ruby] }
        end

        it "enumerates the converted samples" do
            assert_equal @data, @samples.each.to_a
        end

        describe "logical time" do
            it "skips samples before 'from'" do
                @samples.from(now + 0.5)
                assert_raw_samples_equal @data[1..-1], @samples
            end

            it "includes the sample whose logical time is 'from'" do
                @samples.from(now + 1)
                assert_raw_samples_equal @data[1..-1], @samples
            end

            it "stops enumerating when the logical time reaches 'to'" do
                @samples.to(now + 2.5)
                assert_raw_samples_equal @data[0..-2], @samples
            end

            it "includes the sample whose logical time is 'to'" do
                @samples.to(now + 2)
                assert_raw_samples_equal @data[0..-2], @samples
            end

            it "enumerates at most one sample per 'every' period" do
                @samples.every(Time.at(1.1))
                assert_raw_samples_equal (@data[0, 1] + @data[2, 2]), @samples
            end

            it "starts at the first sample that has been yield" do
                @samples.from(now + 0.5)
                @samples.every(Time.at(1.1))
                assert_raw_samples_equal [@data[1], @data[3]], @samples
            end
        end

        describe "realtime" do
            before do
                @samples.realtime
            end

            it "skips samples before 'from'" do
                @samples.from(now + 105/100r)
                assert_raw_samples_equal @data[1..-1], @samples
            end

            it "includes the sample whose logical time is 'from'" do
                @samples.from(now + 11/10r)
                assert_raw_samples_equal @data[1..-1], @samples
            end

            it "stops enumerating when the logical time reaches 'to'" do
                @samples.to(now + 195/100r)
                assert_raw_samples_equal @data[0..-2], @samples
            end

            it "includes the sample whose logical time is 'to'" do
                @samples.to(now + 19/10r)
                assert_raw_samples_equal @data[0..-2], @samples
            end

            it "enumerates at most one sample per 'every' period" do
                @samples.every(Time.at(1 + 1/10r))
                assert_raw_samples_equal (@data[0, 2] + @data[3, 1]), @samples
            end

            it "starts at the first sample that has been yield" do
                @samples.from(now + 0.5)
                @samples.every(Time.at(1.1))
                assert_raw_samples_equal [@data[1], @data[3]], @samples
            end
        end

        describe "sample index" do
            it "skips samples before 'from'" do
                @samples.from(1)
                assert_raw_samples_equal @data[1..-1], @samples
            end

            it "stops enumerating after the sample whose index is 'to'" do
                @samples.to(2)
                assert_raw_samples_equal @data[0..-2], @samples
            end

            it "enumerates at most one sample per 'every' period" do
                @samples.every(2)
                assert_raw_samples_equal [@data[0], @data[2]], @samples
            end

            it "starts at the first sample that has been yield" do
                @samples.from(1)
                @samples.every(2)
                assert_raw_samples_equal [@data[1], @data[3]], @samples
            end
        end

        def assert_raw_samples_equal(expected, samples)
            actual = samples.raw_each.map do |rt, lg, sample|
                [rt, lg, sample.to_ruby]
            end
            assert_equal expected, actual
        end
    end
end
