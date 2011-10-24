require 'rake'
require './lib/pocolog/version'

task 'default' do
end

begin
    require 'rdoc/task'
    RDoc::Task.new do |rd|
        rd.main = "README.txt"
        rd.rdoc_files.include("README.txt", "lib/**/*.rb")
        rd.rdoc_dir = "doc"
    end
    task 'redocs' => 'rerdoc'
rescue LoadError
    STDERR.puts "INFO: documentation targets disabled as the rdoc gem is not installed"
end

