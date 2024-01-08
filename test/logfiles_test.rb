require 'test_helper'

module Pocolog
    describe Logfiles do
        attr_reader :stream_all_samples
        before do
            @stream_all_samples = []
            create_logfile "test.0.log" do
                create_logfile_stream "all"
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
            it "allows to specify the index dir" do
                flexmock(Pocolog::Logfiles).new_instances.
                    should_receive(:rebuild_and_load_index).once.pass_thru
                open_logfile 'test.0.log', index_dir: logfile_path('cache')
                refute File.exist?(logfile_path('test.0.idx'))
                assert File.exist?(logfile_path('cache', 'test.0.idx'))
                # Verify that the index is valid by reading it
                File.open(logfile_path(logfile_path('cache'), 'test.0.idx')) do |io|
                    Format::Current.read_index(io)
                end
            end
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
            it "rebuilds the index if the file size changed" do
                open_logfile('test.0.log') {}
                create_logfile 'test.0.log' do
                    create_logfile_stream 'all'
                end
                flexmock(Pocolog::Logfiles).new_instances.
                    should_receive(:rebuild_and_load_index).once.
                    pass_thru

                logfile = open_logfile 'test.0.log'
                # Check that the loaded index is valid
                assert_equal [], logfile.stream('all').samples.to_a
            end
            it "generates a valid index" do
                create_logfile 'test.0.log' do
                    10.times do |stream_i|
                        create_logfile_stream "s#{stream_i}"
                        10.times do |i|
                            write_logfile_sample Time.at(i), Time.at(i * 100), i
                        end
                    end
                end
                assert_run_successful(logfile_path('test.0.log'))
                10.times do |stream_i|
                    assert_run_successful(logfile_path('test.0.log'), "-s", "s#{stream_i}")
                end
            end
            it "creates an empty file via #new_file after the Logfiles creation" do
                logfile = Logfiles.new
                path = logfile_path("new_file.0.log")
                logfile.new_file(path.to_s)
                logfile.close
                Logfiles.open(path.to_s)
            end
        end

        describe "#each_block_header" do
            it "enumerates the file's blocks after the prologue" do
                logfile = open_logfile "test.0.log"
                blocks = logfile.each_block_header.to_a
                assert_equal 101, blocks.size
                assert_equal ([STREAM_BLOCK] + [DATA_BLOCK] * 100),
                             blocks.map(&:kind)
                assert(blocks.all? { |b| b.stream_index == 0 })
            end
        end

        describe "#each_data_block_header" do
            it "enumerates the file's data block headers after the prologue" do
                logfile = open_logfile "test.0.log"
                blocks = logfile.each_data_block_header.to_a
                assert_equal 100, blocks.size
                assert(blocks.all? { |index, _| index == 0 })
                rt_times = blocks.map { |_, header| header.rt }
                assert_equal(@stream_all_samples.map { |rt, _, _| rt }, rt_times)
                lg_times = blocks.map { |_, header| header.lg }
                assert_equal(@stream_all_samples.map { |_, lg, _| lg }, lg_times)
            end
        end

        describe "#raw_each" do
            it "enumerates the file's data sequentially, "\
               "not converting the data itself to Ruby" do
                logfile = open_logfile "test.0.log"
                blocks = logfile.raw_each.to_a
                expected = 100.times.map do |i|
                    [0, @stream_all_samples[i][1],
                     Typelib.from_ruby(i, logfile.stream("all").type)]
                end

                assert_equal expected, blocks
            end
        end

        describe "#raw_each" do
            it "enumerates the file's data sequentially, "\
               "converting the data itself to Ruby" do
                logfile = open_logfile "test.0.log"
                blocks = logfile.each.to_a
                expected = 100.times.map do |i|
                    [0, @stream_all_samples[i][1], i]
                end

                assert_equal expected, blocks
            end
        end

        describe ".default_index_filename" do
            it "returns the path with .log changed into .idx" do
                assert_equal "/path/to/file.0.idx", Logfiles.default_index_filename("/path/to/file.0.log")
            end
            it "allows to specify the cache directory" do
                assert_equal "/another/dir/file.0.idx", Logfiles.default_index_filename("/path/to/file.0.log", index_dir: '/another/dir')
            end
            it "raises ArgumentError if the logfile path does not end in .log" do
                assert_raises(ArgumentError) do
                    Logfiles.default_index_filename("/path/to/file.0.log.garbage")
                end
            end
        end

        describe ".encode_stream_declaration_payload" do
            before do
                @registry = Typelib::CXXRegistry.new
                @type = @registry.create_compound "/C" do |b|
                    b.f = "/double"
                end
                @metadata = { "some" => %w[meta data] }
            end

            it "encodes the information into its binary form" do
                encoded = Logfiles.encode_stream_declaration_payload(
                    "stream_name", @type, metadata: @metadata
                )
                assert_decodes_to_expected encoded
            end

            it "handles being given a type name and type registry" do
                metadata = { "some" => %w[meta data] }
                encoded = Logfiles.encode_stream_declaration_payload(
                    "stream_name", "/C", type_registry: @registry, metadata: metadata
                )
                assert_decodes_to_expected encoded
            end

            it "raises if given a type name but no type registry" do
                assert_raises(ArgumentError) do
                    Logfiles.encode_stream_declaration_payload("stream_name", "/C")
                end
            end

            it "handles a type registry already in string form" do
                metadata = { "some" => %w[meta data] }
                encoded = Logfiles.encode_stream_declaration_payload(
                    "stream_name", "/C",
                    type_registry: @registry.to_xml, metadata: metadata
                )
                assert_decodes_to_expected encoded
            end

            def assert_decodes_to_expected(encoded)
                decoded = BlockStream::StreamBlock.parse(encoded)
                assert_equal "stream_name", decoded.name
                assert_equal @type, decoded.type
                assert_equal @metadata, decoded.metadata
            end
        end
    end
end

