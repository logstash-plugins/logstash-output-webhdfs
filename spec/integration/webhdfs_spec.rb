# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe LogStash::Outputs::WebHdfs, :integration => true do
  webhdfs_host = 'localhost'
  webhdfs_port = 50070
  webhdfs_user = 'hadoop'
  path_to_testlog = "/user/#{webhdfs_user}/test.log"
  current_logfile_name = "/user/#{webhdfs_user}/test.log"
  current_config = ""

  event = LogStash::Event.new('message' => 'Hello world!',
                              'source' => 'out of the blue',
                              'type' => 'generator',
                              'host' => 'localhost',
                              '@timestamp' => LogStash::Timestamp.now)

  default_config =  { 'host' => webhdfs_host,
                      'user' => webhdfs_user,
                      'path' => path_to_testlog,
                      'compression' => 'none' }

  client = WebHDFS::Client.new(webhdfs_host, webhdfs_port, webhdfs_user)

  context 'when writing messages' do

    before :each do
      current_logfile_name = path_to_testlog
      current_config = default_config.clone
    end

    after :each do
      client.delete(current_logfile_name)
    end

    it 'should use the correct filename pattern' do
      current_config['path'] = "/user/#{webhdfs_user}/%{host}_test.log"
      current_logfile_name = "/user/#{webhdfs_user}/localhost_test.log"
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      subject.receive(event)
      subject.teardown()
      expect { client.read(current_logfile_name) }.to_not raise_error
    end

    it 'should match the event data' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      subject.receive(event)
      subject.teardown()
      expect(client.read(current_logfile_name).strip()).to eq(event.to_json)
    end

    it 'content should match the configured pattern' do
      current_config['message_format'] = '%{message} came %{source}.'
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      subject.receive(event)
      subject.teardown()
      expect(client.read(current_logfile_name).strip).to eq('Hello world! came out of the blue.')
    end

    it 'should flush after configured idle time' do
      current_config['idle_flush_time'] = 2
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      subject.receive(event)
      expect { client.read(current_logfile_name) }.to raise_error(error=WebHDFS::FileNotFoundError)
      sleep 3
      expect { client.read(current_logfile_name) }.to_not raise_error
      expect(client.read(current_logfile_name).strip()).to eq(event.to_json)
    end

    it 'should write some messages uncompressed' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      for _ in 0..499
        subject.receive(event)
      end
      subject.teardown()
      expect(client.read(current_logfile_name).lines.count).to eq(500)
    end

    it 'should write some messages gzip compressed' do
      current_logfile_name = current_logfile_name + ".gz"
      current_config['compression'] = 'gzip'
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register()
      for _ in 0..499
        subject.receive(event)
      end
      subject.teardown()
      expect(Zlib::Inflate.new(window_bits=47).inflate(client.read(current_logfile_name)).lines.count ).to eq(500)
    end
  end
end
