module Pocolog
    class DataStream
	# Write a sample in this stream, with the +rt+ and +lg+
	# timestamps. +data+ can be either a Typelib::Type object of the
	# right type, or a String (in which case we consider that it is
	# the raw data)
	def write(rt, lg, data)
	    if data.respond_to?(:to_str)
		data = data.to_str
            else
                data = type.from_ruby(data)
                data = data.to_byte_array
	    end

	    logfile.write_data_block(self, rt, lg, data)
	end
    end
end

