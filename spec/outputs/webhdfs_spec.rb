# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'json'

describe 'outputs/webhdfs' do

  webhdfs_host = 'localhost'
  webhdfs_port = 50070
  webhdfs_user = 'hadoop'
  path_to_testlog = '/user/hadoop/test.log'
  current_logfile_name = '/user/hadoop/test.log'
  current_config = ""

      event = LogStash::Event.new(
        'message' => 'Hello world!',
        'source' => 'out of the blue',
        'type' => 'generator',
        'host' => 'localhost',
        '@timestamp' => LogStash::Timestamp.now)

  default_config =  { 'host' => webhdfs_host,
                      'user' => webhdfs_user,
                      'path' => path_to_testlog,
                      'compression' => 'none' }

  client = WebHDFS::Client.new(webhdfs_host, webhdfs_port, webhdfs_user)

  context 'when initializing' do

    it 'should fail to register without required values' do
      expect { LogStash::Plugin.lookup("output", "webhdfs").new() }.to raise_error(error=LogStash::ConfigurationError)
    end

    it 'should register with default values' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(default_config)
      expect { subject.register }.to_not raise_error
    end

    it 'should have default config values' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(default_config)
      insist { subject.port } == 50070
      insist { subject.idle_flush_time } == 1
      insist { subject.flush_size } == 500
      insist { subject.open_timeout } == 30
      insist { subject.read_timeout } == 30
      insist { subject.use_httpfs } == false
      insist { subject.retry_known_errors } == true
      insist { subject.retry_interval } == 0.5
      insist { subject.retry_times } == 5
      insist { subject.snappy_bufsize } == 32768
      insist { subject.snappy_format } == 'stream'
      insist { subject.remove_at_timestamp } == true
    end
  end

  context 'when writing messages' do

    before :each do
      current_logfile_name = path_to_testlog
      current_config = default_config.clone
    end

    it 'should match the event data' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      subject.receive(event)
      subject.teardown
      insist { client.read(current_logfile_name).strip } == event.to_json
    end

    it 'should match the configured pattern' do
      current_config['message_format'] = '%{message} came %{source}.'
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      subject.receive(event)
      subject.teardown
      insist { client.read(current_logfile_name).strip } == 'Hello world! came out of the blue.'
    end

    # Hive does not like a leading "@", but we need @timestamp for path calculation.
    it 'should remove the @timestamp field if configured' do
      current_config['remove_at_timestamp'] = true
      current_config['message_format'] = '%{@timestamp} should be missing.'
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      subject.receive(event)
      subject.teardown
      insist { client.read(current_logfile_name).strip } == '%{@timestamp} should be missing.'
    end

    it 'should flush after configured idle time' do
      current_config['idle_flush_time'] = 2
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      subject.receive(event)
      expect { client.read(current_logfile_name) }.to raise_error(error=WebHDFS::FileNotFoundError)
      sleep 3
      insist { client.read(current_logfile_name).strip } == event.to_json
    end

    it 'should write some messages uncompressed' do
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      for _ in 0..499
        subject.receive(event)
      end
      subject.teardown
      insist { client.read(current_logfile_name).lines.count } == 500
    end

    it 'should write some messages gzip compressed' do
      current_logfile_name = current_logfile_name + ".gz"
      current_config['compression'] = 'gzip'
      subject = LogStash::Plugin.lookup("output", "webhdfs").new(current_config)
      subject.register
      for _ in 0..499
        subject.receive(event)
      end
      subject.teardown
      insist { Zlib::Inflate.new(window_bits=47).inflate(client.read(current_logfile_name)).lines.count } == 500
    end

    after :each do
      client.delete(current_logfile_name)
    end

  end

end
