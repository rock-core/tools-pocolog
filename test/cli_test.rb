require 'test_helper'

module Pocolog
    describe "the pocolog command" do
        before do
            create_logfile 'test.0.log' do
                create_logfile_stream 'stream'
                write_logfile_sample Time.at(10, 100), Time.at(100, 10), 1
                write_logfile_sample Time.at(20, 200), Time.at(200, 20), 2
                write_logfile_sample Time.at(30, 300), Time.at(300, 30), 3

                create_logfile_stream 'stream1'
                write_logfile_sample Time.at(10, 100), Time.at(100, 10), 1
                write_logfile_sample Time.at(20, 200), Time.at(200, 20), 2
                write_logfile_sample Time.at(30, 300), Time.at(300, 30), 3
            end
        end
        def pocolog_bin
            File.expand_path(File.join('..', 'bin', 'pocolog'), __dir__)
        end

        def assert_run_successful(*command, filter_output: true)
            output = IO.popen([pocolog_bin, *command]) do |io|
                io.readlines.map(&:chomp)
            end
            assert $?.success?
            if filter_output
                output.find_all { |line| line !~ /pocolog.rb\[INFO\]: (?:building index|loading file info|done)/ }
            else
                output
            end
        end

        describe 'without arguments' do
            it "outputs a file's summary" do
                output = assert_run_successful(logfile_path('test.0.log'))
                assert_match(/stream.*int32_t/, output[-2])
                assert_match(/3 samples/, output[-1])
            end
        end

        describe '--show' do
            it "outputs a file's stream contents as CSV" do
                output = assert_run_successful(logfile_path('test.0.log'), '--show', 'stream', '--csv')
                assert_equal ["stream", "1", "2", "3"], output
            end

            it "outputs the sample's logical times with --time" do
                output = assert_run_successful(logfile_path('test.0.log'), '--show', 'stream', '--csv', '--time')
                assert_equal ["time stream", "100.000010 1", "200.000020 2", "300.000030 3"], output
            end

            it "outputs the sample's real times with --time --rt" do
                output = assert_run_successful(logfile_path('test.0.log'), '--show', 'stream', '--csv', '--time', '--rt')
                assert_equal ["time stream", "10.000100 1", "20.000200 2", "30.000300 3"], output
            end

            describe '--fields' do
                describe "the handling of compounds" do
                    before do
                        registry = Typelib::Registry.new
                        compound_t = registry.create_compound '/C' do |c|
                            c.add 'a', int32_t
                            c.add 'b', int32_t
                            c.add 'c', int32_t
                        end
                        create_logfile 'select_test.0.log' do
                            create_logfile_stream 'stream', type: compound_t
                            write_logfile_sample Time.at(100), Time.at(10), compound_t.new(a: 1, b: 2, c: 3)
                            write_logfile_sample Time.at(200), Time.at(20), compound_t.new(a: 4, b: 5, c: 6)
                        end
                    end

                    it "selects a single field of a compound to show" do
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', 'b')
                        assert_equal ['b', '2', '5'], output
                    end

                    it "selects multiple fields separated by commans" do
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', 'a,b')
                        assert_equal ['a b', '1 2', '4 5'], output
                    end
                end

                describe "the handling of arrays" do
                    attr_reader :int32_t, :array_t, :compound_t
                    before do
                        registry = Typelib::Registry.new
                        @int32_t = registry.create_numeric '/int32_t', 4, :sint
                        @array_t = registry.create_array int32_t, 2
                        @compound_t = registry.create_compound '/C' do |c|
                            c.add 'a', array_t
                            c.add 'b', int32_t
                            c.add 'c', int32_t
                        end
                    end

                    it "selects all elements of an array with []" do
                        create_logfile 'select_test.0.log' do
                            create_logfile_stream 'stream', type: compound_t
                            write_logfile_sample Time.at(100), Time.at(10), compound_t.new(a: [1, 2], b: 3, c: 4)
                            write_logfile_sample Time.at(200), Time.at(20), compound_t.new(a: [5, 6], b: 7, c: 8)
                        end
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', 'a[]')
                        assert_equal ['a[]', '1 2', '5 6'], output
                    end
                    it "selects specific elements of an array with [INDEX]" do
                        create_logfile 'select_test.0.log' do
                            create_logfile_stream 'stream', type: compound_t
                            write_logfile_sample Time.at(100), Time.at(10), compound_t.new(a: [1, 2], b: 3, c: 4)
                            write_logfile_sample Time.at(200), Time.at(20), compound_t.new(a: [5, 6], b: 7, c: 8)
                        end
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', 'a[0]')
                        assert_equal ['a[0]', '1', '5'], output
                    end
                    it "deals with toplevel arrays" do
                        create_logfile 'select_test.0.log' do
                            create_logfile_stream 'stream', type: array_t
                            write_logfile_sample Time.at(100), Time.at(10), [1, 2]
                            write_logfile_sample Time.at(200), Time.at(20), [3, 4]
                        end
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', '[]')
                        assert_equal ['[]', '1 2', '3 4'], output
                        output = assert_run_successful(logfile_path('select_test.0.log'), '--show', 'stream', '--fields', '[0]')
                        assert_equal ['[0]', '1', '3'], output
                    end
                end

                it "selects all elements of a container with []" do
                end
                it "selects specific elements of a container with [INDEX]" do
                end
                it "deals with toplevel containers" do
                end
            end

            describe '--expr' do
                it "displays the output of the given ruby expression" do
                    output = assert_run_successful(logfile_path('test.0.log'), '--show', 'stream', '--csv', '--expr', 'sample*2')
                    assert_equal ["2", "4", "6"], output
                end
            end
        end
    end
end

