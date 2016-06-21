module Pocolog
    module Upgrade
        module Ops
            class NumericCast < Base
                # Minimum value allowed in {#to_type} if a range check is needed
                #
                # @return [Numeric,nil] the minimum value, or nil if the source
                #   type cannot cause a range error
                attr_reader :range_min

                # Maximum value allowed in {#to_type} if a range check is needed
                #
                # @return [Numeric,nil] the maximum value, or nil if the source
                #   type cannot cause a range error
                attr_reader :range_max

                def initialize(from_t, to_t)
                    super(to_t)
                    if from_t.integer?
                        from_range = compute_integer_range(from_t)
                    end
                    if to_t.integer?
                        to_range   = compute_integer_range(to_t)
                    end
                    if !from_range
                        if to_range
                            @range_min, @range_max = *to_range
                        end
                    elsif to_range && (from_range.first < to_range.first || from_range.last > to_range.last)
                        @range_min, @range_max = *to_range
                    end
                end

                # @api private
                #
                # Compute the range for the given type
                def compute_integer_range(type)
                    if type.unsigned?
                        [0, 2**type.size]
                    else
                        limit = 2**(type.size - 1)
                        [-(limit-1), limit]
                    end
                end

                def convert(value)
                    ruby_value = Typelib.to_ruby(value)
                    if range_min
                        if ruby_value < range_min
                            raise RangeError, "value below minimum value for #{to_type}"
                        elsif ruby_value > range_max
                            raise RangeError, "value above maximum value for #{to_type}"
                        end
                    end
                    Typelib.from_ruby(ruby_value, to_type)
                end
            end
        end
    end
end
