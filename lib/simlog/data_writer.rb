module Pocosim
    class DataStream
	# Write a sample in this stream, with the +rt+ and +lg+
	# timestamps. +data+ can be either a Typelib::Type object of the
	# right type, or a String (in which case we consider that it is
	# the raw data)
	def write(rt, lg, data)
	    if data.kind_of?(Typelib::Type)
		if data.class == self.type
		    data = data.to_byte_array
		else
		    raise ArgumentError, "wrong data type #{data}, expected #{self.stream_type}"
		end
	    elsif data.respond_to?(:to_str)
		data = data.to_str
	    else
		raise ArgumentError, "wrong data type #{data}"
	    end

	    logfile.write_data_block(self, rt, lg, data)
	end
    end
end

