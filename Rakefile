require 'rake'
require './lib/pocolog/version'
require 'rdoc'

task 'default'

begin
    require 'hoe'
    Hoe.spec 'pocolog' do
        developer "Sylvain Joyeux", "sylvain.joyeux@m4x.org"

        self.summary = 'Manipulation library of binary log files'
        self.description = paragraphs_of('README.txt', 3..6).join("\n\n")
        self.url          = paragraphs_of('README.txt', 0).first.split(/\n/)[1..-1]
        self.changes      = paragraphs_of('Changes.txt', 0).join("\n\n")

        extra_deps << ['utilrb', '>= 0.0']
    end

rescue Exception => e
    if !e.message =~ /\.rubyforge/
        STDERR.puts "cannot load the Hoe gem, or Hoe fails. Distribution is disabled"
        STDERR.puts "error message is: #{e.message}"
    end
end

