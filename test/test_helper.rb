# simplecov must be loaded FIRST. Only the files required after it gets loaded
# will be profiled !!!
if ENV['TEST_ENABLE_COVERAGE'] == '1'
    begin
        require 'simplecov'
        SimpleCov.start do
            add_filter "test"
        end
    rescue LoadError
        require 'pocolog'
        Pocolog.warn "coverage is disabled because the 'simplecov' gem cannot be loaded"
    rescue Exception => e
        require 'pocolog'
        Pocolog.warn "coverage is disabled: #{e.message}"
    end
end

require 'pocolog'
require 'minitest/autorun'
require 'minitest/spec'
require 'flexmock/minitest'
FlexMock.partials_are_based = true

if ENV['TEST_ENABLE_PRY'] != '0'
    begin
        require 'pry'
        if ENV['TEST_DEBUG'] == '1'
            require 'pry-rescue/minitest'
        end
    rescue Exception
        Pocolog.warn "debugging is disabled because the 'pry' gem cannot be loaded"
    end
end

Pocolog.logger.level = Logger::FATAL

module Pocolog
    # This module is the common setup for all tests
    #
    # It is included in all the minitest tests
    #
    # @example
    #   require 'pocolog/test'
    #   describe Pocolog do
    #       # Use helpers methods from SelfTest here
    #   end
    #
    module SelfTest
        attr_reader :double_t

        # Common setup code for all pocolog tests
        def setup
            @double_t = Typelib::Registry.new.create_numeric '/double', 8, :float
        end

        # Common teardown code for all pocolog tests
        def teardown
            FileUtils.rm_f 'test.0.log'
            FileUtils.rm_f 'test.0.idx'
        end

        def open_logfile
            FileUtils.rm_f 'test.0.log'
            FileUtils.rm_f 'test.0.idx'
            @logfile = Pocolog::Logfiles.create('test')
        end

        def close_logfile
            @logfile.close
            @logfile = nil
        end

        def create_log_stream(name, data = Array.new, metadata: Hash.new)
            stream = @logfile.create_stream(name, double_t, metadata)
            data.each do |v|
               stream.write(Time.at(v * 10), Time.at(v * 10), v)
            end
            stream
        end
    end
end

class Minitest::Test
    include Pocolog::SelfTest
end


