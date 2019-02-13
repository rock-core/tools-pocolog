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
require 'pocolog/test_helpers'
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
        include Pocolog::TestHelpers

        def pocolog_bin
            File.expand_path(File.join('..', 'bin', 'pocolog'), __dir__)
        end

        def assert_run_successful(*command)
            output = IO.popen([pocolog_bin, *command]) do |io|
                io.readlines.map(&:chomp)
            end
            assert $?.success?
            output.find_all do |line|
                line !~ /pocolog.rb\[INFO\]: (?:building index|loading file info|done)/
            end
        end
    end
end

class Minitest::Test
    include Pocolog::SelfTest
end


