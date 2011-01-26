require 'rake'
require './lib/pocolog/version'
require 'rdoc/task'

task 'default' do
end

RDoc::Task.new do |rd|
    rd.main = "README.txt"
    rd.rdoc_files.include("README.txt", "lib/**/*.rb")
    rd.rdoc_dir = "doc"
end
task 'redocs' => 'rerdoc'

