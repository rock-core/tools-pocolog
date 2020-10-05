require 'test_helper'
require 'pocolog/upgrade/dsl'

module Pocolog
    module Upgrade
        describe DSL do
            describe ".create" do
                attr_reader :int_t, :double_t, :converter_registry
                before do
                    @int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    @double_t = Typelib::Registry.new.create_numeric '/int', 8, :float
                    @converter_registry = ConverterRegistry.new
                end
                it "creates a converter that does nothing" do
                    Dir.mktmpdir do |dir|
                        dir = Pathname.new(dir)
                        ref_time = Time.now.to_date
                        DSL.create(dir, ref_time, int_t, double_t)
                        converters = DSL.load_dir(dir, converter_registry)
                        assert_equal 1, converters.size
                        c = converters.first
                        assert_equal ref_time.to_time, c.time_to
                        assert_equal int_t, c.from_type
                        assert_equal double_t, c.to_type
                        source, target = flexmock, flexmock
                        assert_equal target, c.call(target, source)
                    end
                end

                it "saves the source and target TLBs" do
                    Dir.mktmpdir do |dir|
                        dir = Pathname.new(dir)
                        ref_time = Time.now.to_date
                        DSL.create(dir, ref_time, int_t, double_t)
                        assert_equal int_t.to_xml, (dir + "#{ref_time.iso8601}:int.1.source.tlb").read
                        assert_equal double_t.to_xml, (dir + "#{ref_time.iso8601}:int.1.target.tlb").read
                    end
                end

                it "adds a numerical suffix, and creates the file using the existing max suffix plus one" do
                    Dir.mktmpdir do |dir|
                        dir = Pathname.new(dir)
                        ref_time = Time.now.to_date
                        DSL.create(dir, ref_time, int_t, double_t)
                        DSL.create(dir, ref_time, int_t, double_t)
                        assert_equal (dir + "#{ref_time.iso8601}:int.1").read, (dir + "#{ref_time.iso8601}:int.2").read
                        assert_equal int_t.to_xml, (dir + "#{ref_time.iso8601}:int.2.source.tlb").read
                        assert_equal double_t.to_xml, (dir + "#{ref_time.iso8601}:int.2.target.tlb").read
                    end
                end
            end

            describe ".load_dir" do
                attr_reader :converter_registry, :fixture_dir
                before do
                    @converter_registry = ConverterRegistry.new
                    @fixture_dir = File.join(__dir__, 'fixtures')
                end

                it "loads all the converters present in the given directory" do
                    converters = DSL.load_dir(fixture_dir, converter_registry)
                    assert_equal 1, converters.size
                    assert_equal Time.new(1970, 1, 1), converters[0].time_to

                    source_xml = File.read(File.join(fixture_dir, "1970-01-01:test_t.1.source.tlb"))
                    assert_equal Typelib::Registry.from_xml(source_xml).get('/test_t'), converters[0].from_type
                    target_xml = File.read(File.join(fixture_dir, "1970-01-01:test_t.1.target.tlb"))
                    assert_equal Typelib::Registry.from_xml(target_xml).get('/test_t'), converters[0].to_type
                end
            end
        end
    end
end

