# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe 'outputs/webhdfs' do

  let(:host) { 'localhost' }
  let(:user) { 'hadoop' }
  let(:path) { '/test.log' }

  let(:config) { { 'host' =>host, 'user' => user, 'path' => path, 'compression' => 'none' } }

  subject(:plugin) { LogStash::Plugin.lookup("output", "webhdfs").new(config) }

  describe '#initializing' do

    it 'should fail to register without required values' do
      plugin = LogStash::Plugin.lookup("output", "webhdfs")
      expect { plugin.new }.to raise_error(error=LogStash::ConfigurationError)
    end

    context "default values" do

      it 'should have default port' do
        expect(subject.port).to eq(50070)
      end

      it 'should have default idle_flush_time' do
        expect(subject.idle_flush_time).to eq(1)
      end
      it 'should have default flush_size' do
        expect(subject.flush_size).to eq(500)
      end

      it 'should have default open_timeout' do
        expect(subject.open_timeout).to eq(30)
      end

      it 'should have default read_timeout' do
        expect(subject.read_timeout).to eq(30)
      end

      it 'should have default use_httpfs' do
        expect(subject.use_httpfs).to eq(false)
      end

      it 'should have default retry_known_errors' do
        expect(subject.retry_known_errors).to eq(true)
      end

      it 'should have default retry_interval' do
        expect(subject.retry_interval).to eq(0.5)
      end

      it 'should have default retry_times' do
        expect(subject.retry_times).to eq(5)
      end

      it 'should have default snappy_bufsize' do
        expect(subject.snappy_bufsize).to eq(32768)
      end

      it 'should have default snappy_format' do
        expect(subject.snappy_format).to eq('stream')
      end

    end
  end
end
