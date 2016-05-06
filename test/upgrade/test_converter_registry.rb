require 'log_tools/test'
require 'log_tools/upgrade'

module Pocolog
    module Upgrade
        describe ConverterRegistry do
            attr_reader :registry, :int_t, :double_t, :base_time
            before do
                @registry = ConverterRegistry.new
                @int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                @double_t = Typelib::Registry.new.create_numeric '/double', 4, :float
                @base_time = Time.now
            end

            describe "#build_converter_graph" do
                it "ignores converters that are not valid for the given time" do
                    minus_05 = registry.add(base_time - 05, int_t, int_t) {}
                    minus_10 = registry.add(base_time - 10, int_t, int_t) {}
                    minus_15 = registry.add(base_time - 15, int_t, int_t) {}

                    graph = registry.build_converter_graph(Time.now - 12)
                    edges = graph.enum_for(:each_edge).to_a
                    expected = [[minus_10, minus_05]]
                    assert_equal Set[*expected], edges.to_set
                end
                it "adds edges between the latest converter that has a to_type matching the earliest converter that has a matching from_type" do
                    minus_05_convert = registry.add(base_time - 05, int_t, double_t) {}
                    minus_10_int_upgrade = registry.add(base_time - 10, int_t, int_t) {}
                    minus_15_convert = registry.add(base_time - 15, int_t, double_t) {}
                    minus_20_int_upgrade = registry.add(base_time - 20, int_t, int_t) {}
                    minus_10_double_upgrade = registry.add(base_time - 10, double_t, double_t) {}

                    graph = registry.build_converter_graph(Time.now - 30)
                    edges = graph.enum_for(:each_edge).to_a
                    expected = [
                        [minus_20_int_upgrade, minus_15_convert],
                        [minus_10_int_upgrade, minus_05_convert],
                        [minus_15_convert, minus_10_double_upgrade]
                    ]

                    assert_equal Set[*expected], edges.to_set
                end
            end

            describe "#compute_source_conversions" do
                attr_reader :graph, :converter
                before do
                    @graph = RGL::DirectedAdjacencyGraph.new
                    @converter = registry.add base_time, int_t, double_t
                end

                it "links the equivalent type to the converters if the type is known" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    v, _ = registry.compute_source_conversions(graph, base_time - 1, eq_int_t)
                    assert_equal [[v, converter]], graph.enum_for(:each_edge).to_a
                end
                it "links the type to known types of the same name that are valid deep casts" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 8, :sint
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time - 1, eq_int_t, int_t, registry).
                        and_return(deep_cast_op = flexmock)
                    v, _ = registry.compute_source_conversions(graph, base_time - 1, eq_int_t)
                    assert_equal Set[[v, deep_cast_op], [deep_cast_op, converter]],
                        graph.enum_for(:each_edge).to_set
                end
                it "ignores types that are known because they are conversion targets" do
                    registry.compute_source_conversions(graph, base_time - 1, double_t)
                    assert graph.empty?
                end
                it "ignores types of the same name that are invalid deep casts" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 8, :sint
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time - 1, eq_int_t, int_t, registry).
                        and_raise(InvalidCast)
                    registry.compute_source_conversions(graph, base_time - 1, eq_int_t)
                    assert_equal [], graph.enum_for(:each_edge).to_a
                end
                it "filters converters by time" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    registry.compute_source_conversions(graph, base_time + 1, eq_int_t)
                    assert_equal [], graph.enum_for(:each_edge).to_a
                end
            end

            describe "#compute_target_conversions" do
                attr_reader :graph, :converter
                before do
                    @graph = RGL::DirectedAdjacencyGraph.new
                    @converter = registry.add base_time, double_t, int_t
                end

                it "links the equivalent type to the converters if the type is known" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    v, _ = registry.compute_target_conversions(graph, eq_int_t)
                    assert_equal [[converter, v]], graph.enum_for(:each_edge).to_a
                end
                it "ignores types that are known because they are conversion sources" do
                    registry.compute_target_conversions(graph, double_t)
                    assert graph.empty?
                end
                it "links the type to known types of the same name that are valid deep casts" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 8, :sint
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time, int_t, eq_int_t, registry).
                        and_return(deep_cast_op = flexmock)
                    v, _ = registry.compute_target_conversions(graph, eq_int_t)
                    assert_equal Set[[converter, deep_cast_op], [deep_cast_op, v]],
                        graph.enum_for(:each_edge).to_set
                end
                it "ignores types of the same name that are invalid deep casts" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 8, :sint
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time, int_t, eq_int_t, registry).
                        and_raise(InvalidCast)
                    registry.compute_target_conversions(graph, eq_int_t)
                    assert_equal [], graph.enum_for(:each_edge).to_a
                end
            end

            describe "#find_converter_chain" do
                it "returns identity for equivalent types that are unknown to the registry" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    ops = registry.find_converter_chain(base_time - 1, int_t, eq_int_t)
                    assert_equal 1, ops.size
                    assert_kind_of Ops::Identity, ops.first
                end
                it "returns identity for equivalent types that are known to the registry" do
                    eq_int_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                    converter = registry.add base_time, int_t, double_t
                    ops = registry.find_converter_chain(base_time - 1, int_t, eq_int_t)
                    assert_equal 1, ops.size
                    assert_kind_of Ops::Identity, ops.first
                end
                it "returns a converter chain if there is one and both types have converters" do
                    converter_0 = registry.add base_time, int_t, int_t
                    converter_1 = registry.add base_time + 1, int_t, double_t
                    ops = registry.find_converter_chain(base_time - 1, int_t, double_t)
                    assert_equal [converter_0, converter_1], ops
                end
                it "returns nil if both types have converters but no chain exists" do
                    converter_0 = registry.add base_time, int_t, int_t
                    converter_1 = registry.add base_time + 1, double_t, double_t
                    assert !registry.find_converter_chain(base_time - 1, int_t, double_t)
                end
                it "attempts a deep cast if the two types are not equivalent and are unknown to the registry" do
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time - 1, int_t, double_t, registry).
                        and_return(cast_ops = flexmock)
                    ops = registry.find_converter_chain(base_time - 1, int_t, double_t)
                    assert_equal [cast_ops], ops
                end
                it "returns nil if the deep cast of two unregistered types fail" do
                    flexmock(Upgrade).should_receive(:build_deep_cast).
                        with(base_time - 1, int_t, double_t, registry).
                        and_raise(Upgrade::InvalidCast)
                    assert !registry.find_converter_chain(base_time - 1, int_t, double_t)
                end
            end
        end
    end
end
