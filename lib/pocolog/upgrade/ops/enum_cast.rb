module LogTools
    module Upgrade
        module Ops
            class EnumCast < Base
                attr_reader :known_symbols

                def initialize(from_t, to_t)
                    super(to_t)
                    @known_symbols = to_t.keys.keys.map(&:to_sym).to_set
                    if from_t.keys.none? { |sym, _| known_symbols.include?(sym.to_sym) }
                        raise InvalidCast, "no common symbols between the two types"
                    end
                end

                def convert(value)
                    symbol = Typelib.to_ruby(value)
                    if !known_symbols.include?(symbol)
                        raise InvalidCast, "#{symbol} is not present in the target enum"
                    end
                    Typelib.from_ruby(symbol, to_type)
                end
            end
        end
    end
end
