module Pocolog
    class DataStream
	# Write a sample in this stream, with the +rt+ and +lg+
	# timestamps. +data+ can be either a Typelib::Type object of the
	# right type, or a String (in which case we consider that it is
	# the raw data)
	def write(rt, lg, data)
            data = Typelib.from_ruby(data, type)
            write_raw(rt, lg, data.to_byte_array)
	end

        # Write an already marshalled sample. +data+ is supposed to be a
        # typelib-marshalled value of the stream type
        def write_raw(rt, lg, data)
            pos = logfile.wio.tell
	    logfile.write_data_block(self, rt, lg, data)
            info.append_sample(logfile.wio_index, pos, rt, lg)
        end
    end
end

