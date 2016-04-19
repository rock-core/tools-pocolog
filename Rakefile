require "bundler/gem_tasks"
require "rake/testtask"

task :default

Rake::TestTask.new(:test) do |t|
    t.libs << "lib"
    t.libs << "."
    t.ruby_opts << '-w'
    t.test_files = FileList['test/**/test_*.rb']
end

task :gem => :build
