module Pocolog
    module Upgrade
        # Class that holds a set of custom convertion
        #
        # It allows to somewhat efficiently query which converters are available
        # for a given (time, type) pair through {#find_all_for}
        class ConverterRegistry
            # The converters indexed by their {Ops::Custom#from_type}
            attr_reader :converters_by_from_type

            # The converters indexed by their {Ops::Custom#to_type}
            attr_reader :converters_by_to_type

            # The converters source types indexed by name
            attr_reader :type_by_type_name

            def initialize
                @converters_by_from_type = Hash.new { |h, k| h[k] = Array.new }
                @converters_by_to_type = Hash.new { |h, k| h[k] = Array.new }
                @type_by_type_name = Hash.new { |h, k| h[k] = Set.new }
            end

            def empty?
                converters_by_from_type.empty?
            end

            # Add a custom converter to this registry
            #
            # This is a convenience method to create and add a {Ops::Custom}
            # object in one go
            def add(time, source_t, to_type, name: nil, &converter)
                register(converter = Ops::Custom.new(time, source_t, to_type, converter))
                converter
            end

            # @api private
            #
            # Finds an already registered type that is equal to a given type, to
            # simplify type resolution in {#find_all_for_type}
            #
            # @param [Typelib::Type] type
            # @return [nil,Typelib::Type]
            def find_equivalent_type(type)
                if candidates = type_by_type_name.fetch(type.name, nil)
                    candidates.find { |t| t == type }
                end
            end

            # Add a custom converter to this registry
            def register(custom_converter)
                # Check whether there is a known type with the same definition
                #
                # This is meant to reduce the type spent resolving types
                if from_type = find_equivalent_type(custom_converter.from_type)
                    custom_converter.from_type = from_type
                end
                if to_type = find_equivalent_type(custom_converter.to_type)
                    custom_converter.to_type = to_type
                end

                from_type = custom_converter.from_type
                converters_by_from_type[from_type] << custom_converter
                type_by_type_name[from_type.name] << from_type
                to_type   = custom_converter.to_type
                converters_by_to_type[to_type] << custom_converter
                type_by_type_name[to_type.name] << to_type
            end

            # @api private
            #
            # Find a way to link the given type to the converter graph as a
            # conversion source
            #
            # It checks whether 'type' itself is known, and if not tries to find
            # the types that have the same name and can into which 'type' can be
            # converted.
            def compute_source_conversions(graph, time, type, relax: false)
                source_vertex = Object.new
                if self_type = find_equivalent_type(type)
                    type = self_type
                    candidates = [self_type]
                else
                    candidates = type_by_type_name.fetch(type.name, Array.new)
                end

                failures = Array.new
                candidates.each do |candidate_type|
                    converters = converters_by_from_type.fetch(candidate_type, Array.new)
                    if converter = converters.sort_by(&:time_to).find { |c| c.time_to > time }
                        if type == converter.from_type
                            graph.add_edge(source_vertex, converter)
                        else
                            begin
                                cast_op = Upgrade.build_deep_cast(time, type, candidate_type, self, relax: relax)
                                graph.add_edge(source_vertex, cast_op)
                                graph.add_edge(cast_op, converter)
                            rescue InvalidCast => e
                                failures << e
                            end
                        end
                    end
                end
                return source_vertex, type, failures
            end

            # @api private
            #
            # Find a way to link the given type to the converter graph as a
            # conversion target
            #
            # It checks whether 'type' itself is known, and if not tries to find
            # the types that have the same name and can be converted to it.
            def compute_target_conversions(graph, type, relax: false)
                target_vertex = Object.new
                if self_type = find_equivalent_type(type)
                    type = self_type
                    candidates = [self_type]
                else
                    candidates = type_by_type_name.fetch(type.name, Array.new)
                end

                failures = Array.new
                candidates.each do |candidate_type|
                    converter = converters_by_to_type.fetch(candidate_type, Array.new).
                        sort_by(&:time_to).last

                    if !converter
                        next
                    elsif candidate_type == type
                        graph.add_edge(converter, target_vertex)
                    else
                        begin
                            deep_cast = Upgrade.build_deep_cast(converter.time_to, candidate_type, type, self, relax: relax)
                            graph.add_edge(converter, deep_cast)
                            graph.add_edge(deep_cast, target_vertex)
                        rescue InvalidCast => e
                            failures << e
                        end
                    end
                end
                return target_vertex, type, failures
            end

            # Enumerate all registered converters
            def each_converter(&block)
                return enum_for(__method__) if !block
                converters_by_from_type.each_value do |converters|
                    converters.each(&block)
                end
            end

            # @api private
            #
            # Builds a directed graph that represents how the converters can be
            # chained
            def build_converter_graph(time)
                graph = RGL::DirectedAdjacencyGraph.new
                each_converter do |converter|
                    next if converter.time_to < time

                    converters = converters_by_from_type.fetch(converter.to_type, Array.new)
                    if next_converter = converters.sort_by(&:time_to).find { |c| converter.time_to < c.time_to }
                        graph.add_edge(converter, next_converter)
                    end
                end
                graph
            end

            # Finds a suitable converter chain to go from 'from_type' to
            # 'to_type'
            #
            # @return [Array<Ops::Base>,nil] either a chain of conversions that
            #   need to be applied, or nil if no conversions couldbe computed
            def find_converter_chain(time, from_type, to_type, relax: false)
                graph = build_converter_graph(time)
                source_v, from_type, from_failures = compute_source_conversions(graph, time, from_type, relax: relax)
                target_v, to_type, to_failures     = compute_target_conversions(graph, to_type, relax: relax)

                if !graph.include?(source_v) || !graph.include?(target_v)
                    if from_type == to_type
                        return [Ops::Identity.new(to_type)], Array.new
                    end

                    begin
                        return [Upgrade.build_deep_cast(time, from_type, to_type, self, relax: false)], Array.new
                    rescue InvalidCast => e
                        return nil, from_failures + to_failures + [e]
                    end
                elsif chain = graph.dijkstra_shortest_path(Hash.new(1), source_v, target_v)
                    return chain[1..-2], Array.new
                else
                    return nil, from_failures + to_failures
                end
            end
        end
    end
end

