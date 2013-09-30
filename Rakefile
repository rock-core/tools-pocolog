require 'rake'
#require './lib/pocolog/version'

begin
    require 'hoe'
    Hoe::plugin :yard

    config = Hoe.spec 'pocolog' do
        self.developer "Sylvain Joyeux", "sylvain.joyeux@dfki.de"
        self.summary = "Log file manipulation for oroGen's logger component"
        self.description = paragraphs_of('README.markdown', 3..6).join("\n\n")
        self.changes     = paragraphs_of('History.txt', 0..1).join("\n\n")
        licenses << "GPLv2 or later"

        extra_deps <<
            ['utilrb',   '>= 1.3.4'] <<
            ['rake',     '>= 0.8'] <<
            ['rbtree',   '>= 0.3.0'] <<
            ['hoe-yard', '>= 0.1.2']
    end

    task :docs => :yard
    task :redocs => :yard

rescue LoadError
    STDERR.puts "cannot load the Hoe gem. Distribution is disabled"
end

