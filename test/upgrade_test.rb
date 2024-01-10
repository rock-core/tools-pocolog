require 'test_helper'
require 'pocolog/upgrade'

module Pocolog
    describe Upgrade do
        attr_reader :registry, :converter_registry
        before do
            @registry = Typelib::Registry.new
            @converter_registry = Upgrade::ConverterRegistry.new
        end

        describe ".upgrade" do
            it "returns an identity if the two types are matching" do
                source_t = registry.create_numeric '/int', 4, :sint
                target_t = Typelib::Registry.new.create_numeric '/int', 4, :sint
                ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                assert_kind_of Upgrade::Ops::Identity, ops
            end

            it "tries to deep-cast the value if the converter at the end of the chain does not produce the target type" do
                source_t = registry.create_numeric '/int', 4, :sint
                target_t = Typelib::Registry.new.create_numeric '/int', 8, :sint
                time = Time.now
                flexmock(Upgrade).should_receive(:build_deep_cast).with(time, source_t, target_t, converter_registry, relax: false).
                    and_return(op = flexmock)
                assert_equal op, Upgrade.compute(time, source_t, target_t, converter_registry)
            end

            it "applies the same-type converters in time order" do
                recorder = flexmock
                int_t = registry.create_numeric '/int', 4, :sint
                converter_registry.add(Time.now - 5, int_t, int_t) do |target, source|
                    recorder.called(source)
                    Typelib.copy(target, Typelib.from_ruby(30, int_t))
                end
                converter_registry.add(Time.now - 10, int_t, int_t) do |target, source|
                    recorder.called(source)
                    Typelib.copy(target, Typelib.from_ruby(20, int_t))
                end

                recorder.should_receive(:called).ordered.once.
                    with(->(source) { Typelib.to_ruby(source) == 10 })
                recorder.should_receive(:called).ordered.once.
                    with(->(source) { Typelib.to_ruby(source) == 20 })
                ops = Upgrade.compute(Time.now - 20, int_t, int_t, converter_registry)
                result = ops.convert(Typelib.from_ruby(10, int_t))
                assert_kind_of int_t, result
                assert_equal 30, Typelib.to_ruby(result)
            end
        end

        describe 'deep casting' do
            before do
                @target_registry = Typelib::Registry.new
            end

            describe "numerics" do
                it "casts a numeric type" do
                    int_t = registry.create_numeric '/int', 4, :sint
                    double_t = @target_registry.create_numeric '/double', 8, :float
                    ops = Upgrade.compute(Time.now, int_t, double_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby(1, int_t))
                    assert_equal 1, Typelib.to_ruby(result)
                end
                it "casts a vector of numeric types" do
                    int_t = registry.create_numeric '/int', 4, :sint
                    double_t = @target_registry.create_numeric '/double', 8, :float
                    ops = Upgrade.compute(Time.now, int_t, double_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby(1, int_t))
                    assert_equal 1, Typelib.to_ruby(result)
                end
                it "checks for underflows because of signed-ness changes" do
                    int_t = registry.create_numeric '/int', 2, :sint
                    uint_t = @target_registry.create_numeric '/uint', 2, :uint
                    ops = Upgrade.compute(Time.now, int_t, uint_t, converter_registry)
                    assert_raises(RangeError) do
                        ops.convert(Typelib.from_ruby(-100, int_t))
                    end
                end
                it "checks for overflows if the source is a bigger integer" do
                    int32_t = registry.create_numeric '/int32', 4, :sint
                    int8_t = @target_registry.create_numeric '/int8', 1, :sint
                    ops = Upgrade.compute(Time.now, int32_t, int8_t, converter_registry)
                    assert_raises(RangeError) do
                        ops.convert(Typelib.from_ruby(512, int32_t))
                    end
                end
                it "checks for overflows if the source is a float" do
                    double_t = registry.create_numeric '/double', 4, :float
                    int_t = @target_registry.create_numeric '/int', 4, :sint
                    ops = Upgrade.compute(Time.now, double_t, int_t, converter_registry)
                    assert_raises(RangeError) do
                        ops.convert(Typelib.from_ruby(2**33, double_t))
                    end
                end
                it "raises if the target type is not a numeric type" do
                    int_t = registry.create_numeric '/int32', 4, :sint
                    target_t = @target_registry.create_enum '/E' do |e|
                        e.add 'SYM'
                    end
                    assert_raises Upgrade::NoChain do
                        Upgrade.compute(Time.now, int_t, target_t, converter_registry)
                    end
                end
            end

            it "raises if attempting to convert an array to a non-enumerable" do
                element_t = @target_registry.create_numeric '/int32', 4, :sint
                array_t = @target_registry.create_array element_t, 8
                assert_raises(Upgrade::NoChain) do
                    Upgrade.compute(Time.now, array_t, element_t, converter_registry)
                end
            end

            it "raises if attempting to convert a container to a non-enumerable" do
                element_t = @target_registry.create_numeric '/int32', 4, :sint
                container_t = @target_registry.create_container '/std/vector', element_t, 8
                assert_raises(Upgrade::NoChain) do
                    Upgrade.compute(Time.now, container_t, element_t, converter_registry)
                end
            end

            describe "arrays to arrays" do
                attr_reader :element_t
                before do
                    @element_t = registry.create_numeric '/int32', 4, :sint
                    @target_element_t = @target_registry.create_numeric '/int32', 4, :sint
                end
                it "raises at model time if the source has less elements" do
                    source_t = registry.create_array element_t, 8
                    target_t = @target_registry.create_array @target_element_t, 10
                    assert_raises(Upgrade::NoChain) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
                it "raises at model time if the source has more elements" do
                    source_t = registry.create_array element_t, 10
                    target_t = @target_registry.create_array @target_element_t, 8
                    assert_raises(Upgrade::NoChain) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
                it "converts elements one by one if the sizes match" do
                    int4_t = registry.create_numeric '/int4', 4, :sint
                    int8_t = @target_registry.create_numeric '/int8', 8, :uint
                    source_t = registry.create_array int4_t, 3
                    target_t = @target_registry.create_array int8_t, 3
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby([1, 2, 3], source_t))
                    assert_kind_of target_t, result
                    assert_equal [1, 2, 3], result.to_a
                end
            end

            describe "containers to arrays" do
                attr_reader :element_t
                before do
                    @element_t = registry.create_numeric '/int32', 4, :sint
                end
                it "raises ArraySizeMismatch at execution time if the sizes are different" do
                    int4_t = registry.create_numeric '/int4', 4, :sint
                    int8_t = @target_registry.create_numeric '/int8', 8, :uint
                    source_t = registry.create_container '/std/vector', int4_t, 3
                    target_t = @target_registry.create_array int8_t, 3
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    assert_raises(Upgrade::ArraySizeMismatch) do
                        ops.convert(Typelib.from_ruby([1, 2], source_t))
                    end
                end

                it "converts elements one by one if the sizes match" do
                    int4_t = registry.create_numeric '/int4', 4, :sint
                    int8_t = @target_registry.create_numeric '/int8', 8, :uint
                    source_t = registry.create_container '/std/vector', int4_t, 3
                    target_t = @target_registry.create_array int8_t, 3
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby([1, 2, 3], source_t))
                    assert_kind_of target_t, result
                    assert_equal [1, 2, 3], result.to_a
                end
            end

            describe "containers" do
                attr_reader :source_element_t, :target_element_t
                before do
                    @source_element_t = registry.create_numeric '/int32', 4, :sint
                    @target_element_t = @target_registry.create_numeric '/int64', 8, :sint
                end

                it "converts from a container with an identical type" do
                    source_t = registry.create_container "/std/vector", source_element_t
                    target_element_t = @target_registry.create_numeric '/int32_t', 4, :sint
                    assert_equal source_element_t, target_element_t
                    target_t = @target_registry.create_container '/std/vector', target_element_t
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby([1, 2, 3], source_t))
                    assert_kind_of target_t, result
                    assert_equal 3, result.size
                    assert_equal [1, 2, 3], result.to_a
                end

                describe "from an array" do
                    it "resizes the target container and copies elements one by one" do
                        source_t = registry.create_array source_element_t, 3
                        target_t = @target_registry.create_container '/std/vector', target_element_t, 5
                        ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                        result = ops.convert(Typelib.from_ruby([1, 2, 3], source_t))
                        assert_kind_of target_t, result
                        assert_equal 3, result.size
                        assert_equal [1, 2, 3], result.to_a
                    end
                end
            end

            describe "enums" do
                attr_reader :source_t

                before do
                    @source_t = registry.create_enum '/source' do |e|
                        e.add :SYM0, 0
                        e.add :SYM1, 1
                    end
                end

                it "translates symbols to symbols" do
                    target_t = @target_registry.create_enum '/target' do |e|
                        e.add :SYM0, 10
                    end
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    result = ops.convert(Typelib.from_ruby(:SYM0, source_t))
                    assert_kind_of target_t, result
                    assert_equal :SYM0, Typelib.to_ruby(result)
                end
                it "raises InvalidCast at runtime if the source value is a symbol that the target does not have" do
                    target_t = @target_registry.create_enum '/target' do |e|
                        e.add :SYM0, 10
                    end
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    assert_raises(Upgrade::InvalidCast) do
                        ops.convert(Typelib.from_ruby(:SYM1, source_t))
                    end
                end
                it "raises InvalidCast if trying to convert to an enum that has no common symbol" do
                    target_t = @target_registry.create_enum '/target' do |e|
                        e.add :SYM10, 10
                    end
                    assert_raises(Upgrade::InvalidCast) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
                it "raises InvalidCast if trying to convert to a non-enum" do
                    target_t = @target_registry.create_numeric '/int', 2, :sint
                    assert_raises(Upgrade::InvalidCast) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
            end

            describe "compounds" do
                attr_reader :int_t, :int_array_t, :double_t, :source_t
                before do
                    @int_t = registry.create_numeric '/int', 4, :sint
                    @int_array_t = registry.create_array int_t, 3
                    @double_t = registry.create_numeric '/double', 4, :float
                    @target_double_t = @target_registry.create_numeric '/double', 4, :float
                    @source_t = registry.create_compound '/source' do |c|
                        c.add 'a', int_t
                        c.add 'b', int_array_t
                    end
                end
                it "applies the conversion on a per-field basis" do
                    double_array_t = @target_registry.create_array double_t, 3
                    target_t = @target_registry.create_compound '/target' do |c|
                        c.add 'a', @target_double_t
                        c.add 'b', double_array_t
                    end
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    source = Typelib.from_ruby(Hash[a: 10, b: [1, 2, 3]], source_t)
                    target = ops.convert(source)
                    assert_kind_of target_t, target
                    assert_equal source.to_simple_value, target.to_simple_value
                end
                it "raises InvalidCast if the target is not a compound" do
                    target_t = @target_registry.create_numeric '/target', 4, :sint
                    assert_raises(Upgrade::InvalidCast) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
                it "raises InvalidCast if the target compound has non-sequence fields that the source compound does not have" do
                    double_array_t = @target_registry.create_array @target_double_t, 3
                    target_t = @target_registry.create_compound '/target' do |c|
                        c.add 'a', double_t
                        c.add 'b', double_array_t
                        c.add 'c', double_t
                    end
                    assert_raises(Upgrade::InvalidCast) do
                        Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    end
                end
                it "ignores fields in the source that are not in the target" do
                    target_t = @target_registry.create_compound '/target' do |c|
                        c.add 'a', @target_double_t
                    end
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    source = Typelib.from_ruby(Hash[a: 10, b: [1, 2, 3]], source_t)
                    target = ops.convert(source)
                    assert_kind_of target_t, target
                    assert_equal Hash['a' => 10], target.to_simple_value
                end
                it "passes if the target compound only adds sequence fields" do
                    double_array_t = @target_registry.create_array @target_double_t, 3
                    seq_array_t = @target_registry.create_container '/std/vector', @target_double_t
                    target_t = @target_registry.create_compound '/target' do |c|
                        c.add 'a', @target_double_t
                        c.add 'b', double_array_t
                        c.add 'c', seq_array_t
                    end
                    ops = Upgrade.compute(Time.now, source_t, target_t, converter_registry)
                    source = Typelib.from_ruby(Hash[a: 10, b: [1, 2, 3]], source_t)
                    target = ops.convert(source)
                    assert_kind_of target_t, target
                    assert_equal source.to_simple_value.merge('c' => Array.new), target.to_simple_value
                end
            end
        end
    end
end

