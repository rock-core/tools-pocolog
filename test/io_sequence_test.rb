require 'test_helper'

module Pocolog
    describe IOSequence do
        attr_reader :io_sequence

        before do
            @fixture_dir = Dir.mktmpdir
            File.open(io0_path = File.join(@fixture_dir, "first"), 'w')  { |io| io.write "0123456789" }
            io0 = File.open(io0_path)
            File.open(io1_path = File.join(@fixture_dir, "second"), 'w') { |io| io.write "abcdef" }
            io1 = File.open(io1_path)
            @io_sequence = IOSequence.new(io0, io1)
        end
        after do
            FileUtils.rm_rf @fixture_dir
        end

        describe "#seek" do
            it "seeks within the first file" do
                io_sequence.seek(4)
                assert_equal "456", io_sequence.read(3)
            end
            it "seeks within the second file while the current file is the first" do
                io_sequence.seek(12)
                assert_equal "cde", io_sequence.read(3)
            end
            it "does not switch IO if within the target file" do
                io_sequence.seek(12)
                flexmock(io_sequence).should_receive(:select_io_from_pos).never
                io_sequence.seek(14)
                assert_equal "ef", io_sequence.read(2)
            end
            it "raises RangeError if the position is out of bounds" do
                assert_raises(RangeError) do
                    io_sequence.seek(20)
                end
            end
        end

        describe "#read" do
            it "limits read to the current file" do
                assert_equal "0123456789", io_sequence.read(20)
            end
            it "switches to the next IO if attempting to read after the current io was exhausted" do
                io_sequence.read(20)
                assert_equal "abcdef", io_sequence.read(20)
            end
            it "rewinds the next IO if its current position was changed because of a seek" do
                io_sequence.seek(12)
                io_sequence.rewind
                io_sequence.read(20)
                assert_equal "abcdef", io_sequence.read(20)
            end
            it "returns nil at the end of the last file" do
                io_sequence.read(20)
                io_sequence.read(20)
                assert_nil io_sequence.read(20)
            end
        end

        describe "#size" do
            it "returns the aggregated size of the sequence" do
                assert_equal 16, io_sequence.size
            end
        end

        describe "#tell" do
            it "returns the byte position in the sequence" do
                assert_equal 0, io_sequence.tell
                io_sequence.seek(12)
                assert_equal 12, io_sequence.tell
            end
        end
    end
end

