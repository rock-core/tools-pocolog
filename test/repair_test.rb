require_relative 'test_helper'
require 'pocolog/repair'

module Pocolog
    describe '.repair_file' do
        describe 'broken prologue' do
            def self.common_behavior
                it 'keeps an invalid file as-is if keep_invalid_files is true' do
                    path = create_file
                    contents = File.read(path)
                    refute Pocolog.repair_file(path, keep_invalid_files: true,
                                                     reporter: recording_reporter)
                    assert_equal contents, File.read(path)
                    assert_equal [[:error, "#{path}: missing or invalid prologue, "\
                                          'nothing to salvage']],
                                 recording_reporter.messages
                end

                it 'removes an invalid file if keep_invalid_files and backup are false' do
                    path = create_file
                    refute Pocolog.repair_file(path, keep_invalid_files: false,
                                                     backup: false,
                                                     reporter: recording_reporter)
                    refute File.file?(path)
                    refute File.file?("#{path}.broken")
                    assert_equal [[:error, "#{path}: missing or invalid prologue, "\
                                          'nothing to salvage'],
                                  [:error, "deleted #{path}"]],
                                 recording_reporter.messages
                end

                it 'moves an invalid file if keep_invalid_files is false but backup is true' do
                    path = create_file
                    refute Pocolog.repair_file(path, keep_invalid_files: false,
                                                     backup: true,
                                                     reporter: recording_reporter)
                    refute File.file?(path)
                    assert File.file?("#{path}.broken")
                    assert_equal [[:error, "#{path}: missing or invalid prologue, "\
                                          'nothing to salvage'],
                                  [:error, "moved #{path} to #{path}.broken"]],
                                 recording_reporter.messages
                end
            end

            describe 'empty file' do
                def create_file
                    path = logfile_path('test.0.log')
                    FileUtils.touch path
                    path
                end
                common_behavior
            end

            describe 'prologue too short' do
                def create_file
                    path = logfile_path('test.0.log')
                    File.open(path, 'w') do |io|
                        io.write(' ' * (Format::Current::PROLOGUE_SIZE - 1))
                    end
                    path
                end
                common_behavior
            end

            describe 'invalid magic' do
                def create_file
                    path = logfile_path('test.0.log')
                    File.open(path, 'w') do |io|
                        io.write(' ' * Format::Current::PROLOGUE_SIZE)
                    end
                    path
                end
                common_behavior
            end
        end

        it 'leaves a file with only a valid prologue as-is' do
            path = create_logfile 'test.0.log'
            close_logfile
            contents = File.read(path)
            assert Pocolog.repair_file(path, reporter: recording_reporter)
            assert_equal contents, File.read(path)
            assert_report_empty
        end

        describe 'truncated file' do
            it 'handles a file truncated at any point' do
                path = create_logfile 'test.0.log'
                prologue = logfile_tell
                positions = [[[], [[], []], prologue]]
                streams = []
                streams[0] = create_logfile_stream 'a'
                positions << [%w[a], [[], []], logfile_tell]
                streams[1] = create_logfile_stream 'b'
                positions << [%w[a b], [[], []], logfile_tell]

                current = [%w[a b], [[], []], nil]
                samples = [
                    [0, Time.at(0), Time.at(100), 10],
                    [1, Time.at(1), Time.at(101), 20],
                    [1, Time.at(2), Time.at(102), 30],
                    [0, Time.at(3), Time.at(103), 40]
                ]

                samples.each do |stream_i, rt, lg, v|
                    streams[stream_i].write(rt, lg, v)
                    current[1] = current[1].dup
                    current[1][stream_i] += [[rt, lg, v]]
                    current[-1] = logfile_tell
                    positions << current.dup
                end
                close_logfile

                size = File.stat(path).size
                repaired_path = path.gsub(/.0.log$/, '-repaired.0.log')
                while size > prologue
                    positions.pop if size < positions.last[-1]
                    expected_streams, expected_samples, = *positions.last
                    FileUtils.cp path, repaired_path
                    Pocolog.repair_file(repaired_path, backup: false)
                    validate_result_file(repaired_path,
                                         expected_streams, expected_samples)
                    size -= 1
                    File.open(path, 'a') { |io| io.truncate(size) }
                end
            end

            def validate_result_file(path, expected_streams, expected_samples)
                logfile = Logfiles.open(path)
                stream_names = logfile.each_stream.map(&:name)
                assert_equal expected_streams, stream_names

                stream_names.each_with_index do |name, i|
                    stream = logfile.stream(name)
                    assert_equal expected_samples[i], stream.samples.to_a
                end
            end
        end
    end
end
