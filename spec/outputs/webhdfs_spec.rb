# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe 'outputs/webhdfs' do

  webhdfs_host = 'localhost'
  webhdfs_user = 'hadoop'
  path_to_testlog = "/user/#{webhdfs_user}/test.log"

  default_config =  { 'host' => webhdfs_host,
                      'user' => webhdfs_user,
                      'path' => path_to_testlog,
                      'compression' => 'none' }

  context 'when initializing' do

    it 'should fail to register without required values' do
      expect { LogStash::Plugin.lookup("output", "webhdfs").new() }.to raise_error(error=LogStash::ConfigurationError)
    end

    it 'should register with default values' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(default_config)
      expect { subject.register() }.to_not raise_error
    end

    it 'should have default config values' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(default_config)
      expect(subject.port).to eq(50070)
      expect(subject.idle_flush_time).to eq(1)
      expect(subject.flush_size).to eq(500)
      expect(subject.open_timeout).to eq(30)
      expect(subject.read_timeout).to eq(30)
      expect(subject.use_httpfs).to eq(false)
      expect(subject.retry_known_errors).to eq(true)
      expect(subject.retry_interval).to eq(0.5)
      expect(subject.retry_times).to eq(5)
      expect(subject.snappy_bufsize).to eq(32768)
      expect(subject.snappy_format).to eq('stream')
      expect(subject.remove_at_timestamp).to eq(true)
    end
  end
end
