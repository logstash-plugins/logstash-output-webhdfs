# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe LogStash::Outputs::WebHdfs, :integration => true do
  let(:host) { 'localhost' }
  let(:port) { 50070 }
  let(:user) { 'test' }
  let(:test_file) { '/user/' + user + '/%{host}.test' }
  let(:hdfs_file_name) { 'user/' + user + '/localhost.test' }

  let(:config) { { 'host' => host, 'user' => user, 'path' => test_file, 'compression' => 'none' } }

  subject(:plugin) { LogStash::Plugin.lookup("output", "webhdfs").new(config) }

  let(:webhdfs_client) { WebHDFS::Client.new(host, port, user) }

  let(:event) { LogStash::Event.new('message' => 'Hello world!', 'source' => 'out of the blue',
                                    'type' => 'generator', 'host' => 'localhost' ) }

  describe "register and close" do

    it 'should register with default values' do
      expect { subject.register }.to_not raise_error
    end

  end

  describe '#write' do

    let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                     'path' => test_file, 'compression' => 'none' } }

    after(:each) do
      webhdfs_client.delete(hdfs_file_name)
    end

    describe "writing plain files" do

      before(:each) do
        subject.register
        subject.receive(event)
        subject.close
      end

      it 'should use the correct filename pattern' do
        expect { webhdfs_client.read(hdfs_file_name) }.to_not raise_error
      end

      context "using the line codec without format" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'line' } }

        it 'should match the event data' do
          expect(webhdfs_client.read(hdfs_file_name).strip()).to eq(event.to_s)
        end

      end

      context "using the json codec" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'json' } }


        it 'should match the event data' do
          expect(webhdfs_client.read(hdfs_file_name).strip()).to eq(event.to_json)
        end

      end

      context "when flushing events" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10, 'idle_flush_time' => 2,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'json' } }

        before(:each) do
          webhdfs_client.delete(hdfs_file_name)
        end

        it 'should flush after configured idle time' do
          subject.register
          subject.receive(event)
          expect { webhdfs_client.read(hdfs_file_name) }.to raise_error(error=WebHDFS::FileNotFoundError)
          sleep 3
          expect { webhdfs_client.read(hdfs_file_name) }.to_not raise_error
          expect(webhdfs_client.read(hdfs_file_name).strip()).to eq(event.to_json)
        end

      end

    end

    describe "#compression" do

      before(:each) do
        subject.register
        for _ in 0...500
          subject.receive(event)
        end
        subject.close
      end

      context "when using no compression" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'line' } }

        it 'should write some messages uncompressed' do
          expect(webhdfs_client.read(hdfs_file_name).lines.count).to eq(500)
        end

      end

      context "when using gzip compression" do

        let(:config) { { 'host' => host, 'user' => user,
                         'path' => test_file, 'compression' => 'gzip', 'codec' => 'line' } }

        it 'should write some messages gzip compressed' do
          expect(Zlib::Inflate.new(window_bits=47).inflate(webhdfs_client.read("#{hdfs_file_name}.gz")).lines.count ).to eq(500)
          webhdfs_client.delete("#{hdfs_file_name}.gz")
        end
      end
    end
  end
end