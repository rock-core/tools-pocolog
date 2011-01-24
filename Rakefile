require 'rake'
require './lib/pocolog/version'
require 'rdoc/task'

task 'default' do
end

RDoc::Task.new do |rd|
    rd.main = "README.txt"
    rd.rdoc_files.include("README.txt", "lib/**/*.rb")
end
task 'redocs' => 'rerdoc'

