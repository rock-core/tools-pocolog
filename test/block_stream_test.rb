require 'test_helper'

module Pocolog
    describe BlockStream do
        def create_fixture
            registry = Typelib::Registry.new
            int_t = registry.create_numeric '/int', 4, :sint
            logfile = Pocolog::Logfiles.create('test')
            all_values = logfile.create_stream('all', int_t, 'test' => 'value', 'test2' => 'value2')
            @expected_data = Array.new
            100.times do |i|
                all_values.write(Time.at(i), Time.at(i * 100), i)
                expected_data << i
            end
        end

        describe "#skip" do
            attr_reader :block_stream
            before do
                io = StringIO.new("0123456789")
                @block_stream = BlockStream.new(io, buffer_read: 5)
            end

            it "skips bytes in the internal buffer" do
                block_stream.read(1)
                block_stream.skip(2)
                assert_equal "3", block_stream.read(1)
            end
            it "skips across the internal buffer's boundary if necessary" do
                block_stream.read(3)
                block_stream.skip(5)
                assert_equal "8", block_stream.read(1)
            end
        end

        describe "#read_next_block_header" do
            before do
                open_logfile
                s0 = create_log_stream 's0'
                s1 = create_log_stream 's1'
                s0.write(Time.now, Time.now, 0)
                s1.write(Time.now, Time.now, 0)
                close_logfile
            end

            it "skips to the next block" do
                block_stream = BlockStream.new(File.open('test.0.log'))
                block_stream.read_prologue
                assert_equal 0, block_stream.read_next_block_header.stream_index
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "takes into account skipped bytes" do
                block_stream = BlockStream.new(File.open('test.0.log'))
                block_stream.read_prologue
                assert_equal 0, block_stream.read_next_block_header.stream_index
                block_stream.skip 1
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "takes into account read_payload bytes" do
                block_stream = BlockStream.new(File.open('test.0.log'))
                block_stream.read_prologue
                assert_equal 0, block_stream.read_next_block_header.stream_index
                block_stream.read_payload 1
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "raises NotEnoughData if there is not enough data left to read a full header" do
                io = File.open('test.0.log', 'a+')
                block_stream = BlockStream.new(io)
                block_stream.read_prologue
                io.truncate(Format::Current::PROLOGUE_SIZE + 1)
                assert_raises(NotEnoughData) do
                    block_stream.read_next_block_header
                end
            end
        end

        describe "#read_stream_block" do
            attr_reader :io
            attr_reader :block_stream
            before do
                open_logfile
                s0 = create_log_stream 's0', metadata: Hash['test' => 10]
                s1 = create_log_stream 's1'
                s0.write(Time.now, Time.now, 0)
                s1.write(Time.now, Time.now, 0)
                close_logfile
                @io = File.open('test.0.log', 'a+')
                @block_stream = BlockStream.new(io)
                block_stream.read_prologue
            end
            it "returns the stream block info" do
                block_stream.read_next_block_header
                stream_block = block_stream.read_stream_block
                assert_equal 's0', stream_block.name
                assert_equal '/double', stream_block.typename
                assert_equal double_t.to_xml, stream_block.registry_xml
                assert_equal YAML.dump('test' => 10), stream_block.metadata_yaml
            end
            it "raises NotEnoughData if the file is truncated" do
                io.truncate(Format::Current::PROLOGUE_SIZE + Format::Current::BLOCK_HEADER_SIZE + 5)
                block_stream.read_next_block_header
                assert_raises(NotEnoughData) do
                    block_stream.read_stream_block
                end
            end

            describe BlockStream::StreamBlock do
                it "resolves the type" do
                    block_stream.read_next_block_header
                    stream_block = block_stream.read_stream_block
                    assert_equal double_t, stream_block.type
                    assert_same stream_block.type, stream_block.type
                end
                it "resolves the metadata" do
                    block_stream.read_next_block_header
                    stream_block = block_stream.read_stream_block
                    assert_equal Hash['test' => 10], stream_block.metadata
                    assert_same stream_block.metadata, stream_block.metadata
                end
            end
        end
    end
end


