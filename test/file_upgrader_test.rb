require 'test_helper'

module Pocolog
    describe FileUpgrader do
        attr_reader :file_upgrader, :in_t, :out_t
        before do
            registry = Typelib::Registry.new
            @in_t  = registry.create_numeric '/in_t', 4, :sint
            @out_t = registry.create_numeric '/out_t', 4, :float
        end

        describe "compute_stream_copy" do
            before do
                @file_upgrader = FileUpgrader.new(flexmock(call: out_t))
            end
            it "returns Identity for empty streams" do
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', type: in_t
                end
                logfile = open_logfile('test.0.log')
                # This is needed so that we can compare the stream objects
                flexmock(Pocolog::Logfiles).should_receive(:open).with(logfile_path('test.0.log')).
                    and_return(logfile)
                result = file_upgrader.compute_stream_copy(logfile_path('test.0.log'))
                assert_equal 1, result.size
                stream_copy = result.first
                assert_equal logfile.stream('test'), stream_copy.in_stream
                assert_equal out_t, stream_copy.out_type
                assert_kind_of Upgrade::Ops::Identity, stream_copy.ops
            end

            it "uses Upgrade.compute to compute the operations, with the realtime of the first sample as reference" do
                base_time = Time.at(100)
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', type: in_t
                    write_logfile_sample base_time,     base_time + 10, 1
                    write_logfile_sample base_time + 1, base_time + 20, 2
                end
                logfile = open_logfile('test.0.log')
                flexmock(Upgrade).should_receive(:compute).
                    with(base_time, in_t, out_t, file_upgrader.converter_registry).
                    and_return(ops = flexmock)
                # This is needed so that we can compare the stream objects
                flexmock(Pocolog::Logfiles).should_receive(:open).with(logfile_path('test.0.log')).
                    and_return(logfile)

                assert_equal [FileUpgrader::StreamCopy.new(logfile.stream('test'), out_t, ops)],
                    file_upgrader.compute_stream_copy(logfile_path('test.0.log'))
            end
            it "raises if Upgrade.compute does and skip_failures is false" do
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', type: in_t
                    write_logfile_sample Time.now,     Time.now + 10, 1
                end
                logfile = open_logfile('test.0.log')
                flexmock(Upgrade).should_receive(:compute).
                    and_raise(Upgrade::InvalidCast)
                assert_raises(Upgrade::InvalidCast) do
                    file_upgrader.compute_stream_copy(logfile_path('test.0.log'))
                end
            end
            it "warns if Upgrade.compute raises and skip_failures is true" do
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', type: in_t
                    write_logfile_sample Time.now,     Time.now + 10, 1
                end
                logfile = open_logfile('test.0.log')
                flexmock(Upgrade).should_receive(:compute).
                    and_raise(Upgrade::InvalidCast)
                assert_equal [], file_upgrader.compute_stream_copy(logfile_path('test.0.log'), skip_failures: true)
            end
        end

        describe "#can_cp?" do
            attr_reader :in_logfile
            before do
                create_logfile 'test.0.log' do
                    create_logfile_stream 'test', type: in_t
                    write_logfile_sample Time.now,     Time.now + 10, 1
                end
            end

            it "returns true if all streams would be copied" do
                file_upgrader = FileUpgrader.new(flexmock(call: in_t))
                stream_copy = file_upgrader.compute_stream_copy(logfile_path('test.0.log'))
                assert file_upgrader.can_cp?(stream_copy)
            end
            it "returns false if some streams need processing" do
                file_upgrader = FileUpgrader.new(flexmock(call: out_t))
                stream_copy = file_upgrader.compute_stream_copy(logfile_path('test.0.log'))
                refute file_upgrader.can_cp?(stream_copy)
            end
        end

        describe "#upgrade" do
            describe "file copy shortcut" do
                before do
                    create_logfile 'test.0.log' do
                        create_logfile_stream 'test', type: in_t
                        write_logfile_sample Time.now,     Time.now + 10, 1
                    end
                    @file_upgrader = FileUpgrader.new(flexmock(call: out_t))
                end
                it "copies the file if reflink is true and can_cp? returns true" do
                    flexmock(file_upgrader).should_receive(:cp_logfile_and_index).once.pass_thru
                    flexmock(file_upgrader).should_receive(:can_cp?).
                        and_return(true)
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: true)
                end
                it "copies the file's index as well" do
                    flexmock(file_upgrader).should_receive(:cp_logfile_and_index).once.pass_thru
                    flexmock(file_upgrader).should_receive(:can_cp?).
                        and_return(true)
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: true)
                    # Make sure that the upgraded file has an index
                    flexmock(Pocolog::Logfiles).new_instances.should_receive(:rebuild_and_load_index).never
                    open_logfile 'upgraded.0.log'
                end
                it "does not copy the file if reflink is false even if can_cp? returns true" do
                    flexmock(file_upgrader).should_receive(:cp_logfile_and_index).never
                    flexmock(file_upgrader).should_receive(:can_cp?).
                        and_return(true)
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                end
                it "does not copy the file if reflink is true but can_cp? returns false" do
                    flexmock(file_upgrader).should_receive(:cp_logfile_and_index).never
                    flexmock(file_upgrader).should_receive(:can_cp?).
                        and_return(false)
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: true)
                end
            end

            describe "stream upgrade" do
                attr_reader :base_time
                before do
                    @base_time = Time.at(100)
                    create_logfile 'test.0.log' do
                        create_logfile_stream 'test', type: in_t, metadata: Hash['test' => 'metadata']
                        write_logfile_sample base_time,     base_time + 10, 1
                        write_logfile_sample base_time + 1, base_time + 20, 2
                        write_logfile_sample base_time + 2, base_time + 30, 3
                        write_logfile_sample base_time + 3, base_time + 40, 4
                    end
                    @file_upgrader = FileUpgrader.new(flexmock(call: out_t))
                end
                it "creates a stream of the same name and metadata, but with the target type" do
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    out_stream = open_logfile_stream 'upgraded.0.log', 'test'
                    assert_equal 'test', out_stream.name
                    assert_equal out_t, out_stream.type
                    assert_equal Hash['test' => 'metadata'], out_stream.metadata
                end
                it "updates the stream samples" do
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    out_stream = open_logfile_stream 'upgraded.0.log', 'test'
                    expected_samples = Array[
                        base_time,     base_time + 10, 1,
                        base_time + 1, base_time + 20, 2,
                        base_time + 2, base_time + 30, 3,
                        base_time + 3, base_time + 40, 4].each_slice(3).to_a
                    assert_equal expected_samples, out_stream.samples.to_a
                end
                it "builds the target's index" do
                    # NOTE: the reading test checks that the index is valid
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    flexmock(Pocolog::Logfiles).new_instances.should_receive(:rebuild_and_load_index).never
                    open_logfile 'upgraded.0.log'
                end

                it "deletes the target file if the stream upgrade fails" do
                    custom_e = Class.new(Exception)
                    flexmock(BlockStream).new_instances.should_receive(:read_data_block).and_raise(custom_e)
                    assert_raises(custom_e) do
                        file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    end
                    assert !File.file?(logfile_path('upgraded.0.log'))
                    assert !File.file?(logfile_path('upgraded.0.idx'))
                end
            end

            describe "stream copy" do
                attr_reader :base_time
                before do
                    @base_time = Time.at(100)
                    create_logfile 'test.0.log' do
                        create_logfile_stream 'test', type: in_t, metadata: Hash['test' => 'metadata']
                        write_logfile_sample base_time,     base_time + 10, 1
                        write_logfile_sample base_time + 1, base_time + 20, 2
                        write_logfile_sample base_time + 2, base_time + 30, 3
                        write_logfile_sample base_time + 3, base_time + 40, 4
                    end
                    @file_upgrader = FileUpgrader.new(flexmock(call: in_t))
                end
                it "creates a stream of the same name and metadata, but with the target type" do
                    flexmock(in_t).new_instances.should_receive(:from_buffer_direct).never
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    out_stream = open_logfile_stream 'upgraded.0.log', 'test'
                    assert_equal 'test', out_stream.name
                    assert_equal in_t, out_stream.type
                    assert_equal Hash['test' => 'metadata'], out_stream.metadata
                end
                it "copies the stream samples" do
                    flexmock(in_t).new_instances.should_receive(:from_buffer_direct).never
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    out_stream = open_logfile_stream 'upgraded.0.log', 'test'
                    expected_samples = Array[
                        base_time,     base_time + 10, 1,
                        base_time + 1, base_time + 20, 2,
                        base_time + 2, base_time + 30, 3,
                        base_time + 3, base_time + 40, 4].each_slice(3).to_a
                    assert_equal expected_samples, out_stream.samples.to_a
                end
                it "builds the target's index" do
                    # NOTE: the reading test checks that the index is valid
                    file_upgrader.upgrade(logfile_path('test.0.log'), logfile_path('upgraded.0.log'), reflink: false)
                    flexmock(Pocolog::Logfiles).new_instances.should_receive(:rebuild_and_load_index).never
                    open_logfile 'upgraded.0.log'
                end
            end
        end
    end
end

