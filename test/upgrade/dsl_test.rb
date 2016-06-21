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
        end
    end
end

