require 'test_helper'

module Pocolog
    describe BlockStream do
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
            attr_reader :block_stream
            before do
                create_logfile 'test.0.log' do
                    stream0 = create_logfile_stream 's0'
                    stream1 = create_logfile_stream 's1'
                    stream0.write Time.now, Time.now, 0
                    stream1.write Time.now, Time.now, 0
                end
                @block_stream = BlockStream.open(logfile_path('test.0.log'))
                block_stream.read_prologue
            end

            it "skips to the next block" do
                assert_equal 0, block_stream.read_next_block_header.stream_index
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "takes into account skipped bytes" do
                assert_equal 0, block_stream.read_next_block_header.stream_index
                block_stream.skip 1
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "takes into account read_payload bytes" do
                assert_equal 0, block_stream.read_next_block_header.stream_index
                block_stream.read_payload 1
                assert_equal 1, block_stream.read_next_block_header.stream_index
            end
            it "raises NotEnoughData if there is not enough data left to read a full header" do
                io = File.open(logfile_path('test.0.log'), 'a+')
                io.truncate(Format::Current::PROLOGUE_SIZE + 1)
                assert_raises(NotEnoughData) do
                    block_stream.read_next_block_header
                end
            end
            it "returns nil if trying to read at EOF" do
                flexmock(block_stream.io).should_receive(:read).and_return(nil)
                assert_nil block_stream.read_next_block_header
            end
        end

        describe "#read_payload" do
            attr_reader :block_stream, :base_time
            before do
                @base_time = Time.at(100, 10)
                create_logfile 'test.0.log' do
                    stream0 = create_logfile_stream 's0'
                    stream0.write base_time, base_time + 1, 0
                end
                @block_stream = BlockStream.open(logfile_path('test.0.log'))
                block_stream.read_prologue
            end

            it "reads the block's payload" do
                block_stream.read_next_block_header
                block_stream.read_next_block_header
                payload = block_stream.read_payload
                header = BlockStream::DataBlockHeader.parse(payload[0, Format::Current::DATA_BLOCK_HEADER_SIZE])
                assert_equal base_time, header.rt
                assert_equal base_time + 1, header.lg
                assert_equal 0, Typelib.to_ruby(int32_t.from_buffer(payload[Format::Current::DATA_BLOCK_HEADER_SIZE, 4]))
            end
            it "refuses reading more than the block's payload size" do
                block_stream.read_next_block_header
                assert_raises(ArgumentError) do
                    block_stream.read_payload(100_000)
                end
            end
            it "raises NotEnoughData if the actual read returned less than the expected amount" do
                block_stream.read_next_block_header
                block_stream.read_next_block_header
                flexmock(block_stream).should_receive(:read).and_return("  ")
                assert_raises(NotEnoughData) do
                    block_stream.read_payload
                end
            end
            it "raises NotEnoughData if the actual read returned nil" do
                block_stream.read_next_block_header
                block_stream.read_next_block_header
                flexmock(block_stream).should_receive(:read).and_return(nil)
                assert_raises(NotEnoughData) do
                    block_stream.read_payload
                end
            end

        end

        describe "#read_stream_block" do
            attr_reader :io
            attr_reader :block_stream
            before do
                create_logfile 'test.0.log' do
                    create_logfile_stream 's0', metadata: Hash['test' => 10]
                    write_logfile_sample Time.now, Time.now, 0
                    create_logfile_stream 's1'
                    write_logfile_sample Time.now, Time.now, 0
                end
                @io = File.open(logfile_path('test.0.log'), 'a+')
                @block_stream = BlockStream.new(io)
                block_stream.read_prologue
            end
            it "returns the stream block info" do
                block_stream.read_next_block_header
                stream_block = block_stream.read_stream_block
                assert_equal 's0', stream_block.name
                assert_equal '/int32_t', stream_block.typename
                assert_equal int32_t.to_xml, stream_block.registry_xml
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
                    assert_equal int32_t, stream_block.type
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


