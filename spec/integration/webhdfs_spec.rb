# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe LogStash::Outputs::WebHdfs, :integration => true do

  let(:host) { 'localhost' }
  let(:port) { 50070 }
  let(:user) { 'vagrant' }

  let(:test_file) { "/test.file" }

  let(:event) { LogStash::Event.new('message' => 'Hello world!', 'source' => 'out of the blue',
                                    'type' => 'generator', 'host' => 'localhost' ) }

  let(:config) { { 'host' => host, 'user' => user,
                   'path' => test_file, 'compression' => 'none' } }

  subject { LogStash::Plugin.lookup("output", "webhdfs").new(config) }

  let(:client) { WebHDFS::Client.new(host, port, user) }

  describe "register and teardown" do

    it 'should register with default values' do
      expect { subject.register }.to_not raise_error
    end

  end

  describe '#write' do

    let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                     'path' => "/%{host}_test.log", 'compression' => 'none' } }

    after(:each) do
      client.delete(test_file)
    end

    describe "writing plain files" do

      before(:each) do
        subject.register
        subject.receive(event)
        subject.teardown
      end

      it 'should use the correct filename pattern' do
        expect { client.read('localhost_test.log') }.to_not raise_error
      end

      context "using the line codec" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'line' } }

        it 'should match the event data' do
          expect(client.read(test_file).strip()).to eq(event.to_s)
        end
      end

      context "using the json codec" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'json' } }


        it 'should match the event data' do
          expect(client.read(test_file).strip()).to eq(event.to_json)
        end

      end

      context "when flushing events" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10, 'idle_flush_time' => 2,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'json' } }

        before(:each) do
          client.delete(test_file)
        end

        it 'should flush after configured idle time' do
          subject.register
          subject.receive(event)
          expect { client.read(test_file) }.to raise_error(error=WebHDFS::FileNotFoundError)
          sleep 3
          expect { client.read(test_file) }.to_not raise_error
          expect(client.read(test_file).strip()).to eq(event.to_json)
        end
      end

    end

    describe "#compression" do

      before(:each) do
        subject.register
        for _ in 0...500
          subject.receive(event)
        end
        subject.teardown
      end

      context "when using no compression" do

        let(:config) { { 'host' => host, 'user' => user, 'flush_size' => 10,
                         'path' => test_file, 'compression' => 'none', 'codec' => 'line' } }

        it 'should write some messages uncompressed' do
          expect(client.read(test_file).lines.count).to eq(500)
        end

      end

      context "when using gzip compression" do

        let(:config) { { 'host' => host, 'user' => user,
                         'path' => test_file, 'compression' => 'gzip', 'codec' => 'line' } }

        it 'should write some messages gzip compressed' do
          expect(Zlib::Inflate.new(window_bits=47).inflate(client.read("#{test_file}.gz")).lines.count ).to eq(500)
        end
      end

    end

  end
end
