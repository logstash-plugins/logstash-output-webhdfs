# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "stud/buffer"
require "logstash/outputs/webhdfs_helper"

# Summary: Plugin to send logstash events to files in HDFS via webhdfs
# REST API.
#
# This plugin only has a mandatory dependency on the webhdfs gem from 
# Kazuki Ohta and TAGOMORI Satoshi (@see: https://github.com/kzk/webhdfs).
# Optional dependencies are zlib and snappy gem. 
# No jars from hadoop are needed, thus reducing configuration and compatibility
# problems.
#
# If you get an error like:
#
#     Max write retries reached. Exception: initialize: name or service not known {:level=>:error}
#
# make sure, that the hostname of your namenode is resolvable on the host running logstash. When creating/appending
# to a file, webhdfs somtime sends a 307 TEMPORARY_REDIRECT with the HOSTNAME of the machine its running on.
#
# ==== Usage
# This is an example of logstash config:
#
# [source,ruby]
# ----------------------------------
# input {
#   ...
# }
# filter {
#   ...
# }
# output {
#   webhdfs {
#     server => "127.0.0.1:50070"         # (required)
#     path => "/user/logstash/dt=%{+YYYY-MM-dd}/logstash-%{+HH}.log"  # (required)
#     user => "hue"                       # (required)
#   }
# }
# ----------------------------------

class LogStash::Outputs::WebHdfs < LogStash::Outputs::Base

  include Stud::Buffer
  include LogStash::Outputs::WebHdfsHelper

  config_name "webhdfs"

  MAGIC = "\x82SNAPPY\x0".force_encoding Encoding::ASCII_8BIT
  DEFAULT_VERSION = 1
  MINIMUM_COMPATIBLE_VERSION = 1

  # The server name for webhdfs/httpfs connections.
  config :host, :validate => :string, :required => true

  # The server port for webhdfs/httpfs connections.
  config :port, :validate => :number, :default => 50070

  # The Username for webhdfs.
  config :user, :validate => :string, :required => true

  # The path to the file to write to. Event fields can be used here,
  # as well as date fields in the joda time format, e.g.:
  # ....
  #     `/user/logstash/dt=%{+YYYY-MM-dd}/%{@source_host}-%{+HH}.log`
  # ....
  config :path, :validate => :string, :required => true

  # The format to use when writing events to the file. This value
  # supports any string and can include `%{name}` and other dynamic
  # strings.
  #
  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  # Sending data to webhdfs in x seconds intervals.
  config :idle_flush_time, :validate => :number, :default => 1

  # Sending data to webhdfs if event count is above, even if store_interval_in_secs is not reached.
  config :flush_size, :validate => :number, :default => 500

  # WebHdfs open timeout, default 30s.
  config :open_timeout, :validate => :number, :default => 30

  # The WebHdfs read timeout, default 30s.
  config :read_timeout, :validate => :number, :default => 30

  # Use httpfs mode if set to true, else webhdfs.
  config :use_httpfs, :validate => :boolean, :default => false

  # Retry some known webhdfs errors. These may be caused by race conditions when appending to same file, etc.
  config :retry_known_errors, :validate => :boolean, :default => true

  # How long should we wait between retries.
  config :retry_interval, :validate => :number, :default => 0.5

  # How many times should we retry. If retry_times is exceeded, an error will be logged and the event will be discarded.
  config :retry_times, :validate => :number, :default => 5

  # Compress output. One of ['none', 'snappy', 'gzip']
  config :compression, :validate => ["none", "snappy", "gzip"], :default => "none"

  # Set snappy chunksize. Only neccessary for stream format. Defaults to 32k. Max is 65536
  # @see http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
  config :snappy_bufsize, :validate => :number, :default => 32768

  # Set snappy format. One of "stream", "file". Set to stream to be hive compatible.
  config :snappy_format, :validate => ["stream", "file"], :default => "stream"

  ## Set codec.
  default :codec, 'line'

  public

  def register
    load_module('webhdfs')
    if @compression == "gzip"
      load_module('zlib')
    elsif @compression == "snappy"
      load_module('snappy')
    end
    @files = {}
    @client = prepare_client(@host, @port, @user)
    # Test client connection.
    begin
      @client.list('/')
    rescue => e
      @logger.error("Webhdfs check request failed. (namenode: #{@client.host}:#{@client.port}, Exception: #{e.message})")
      raise
    end
    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
    @codec.on_event do |event, encoded_event|
      encoded_event
    end
  end # def register

  def receive(event)
    return unless output?(event)
    buffer_receive(event)
  end # def receive

  def flush(events=nil, teardown=false)
    return if not events
    newline = "\n"
    output_files = Hash.new { |hash, key| hash[key] = "" }
    events.collect do |event|
      path = event.sprintf(@path)
      event_as_string = @codec.encode(event)
      event_as_string += newline unless event_as_string.end_with? newline
      output_files[path] << event_as_string
    end
    output_files.each do |path, output|
      if @compression == "gzip"
        path += ".gz"
        output = compress_gzip(output)
      elsif @compression == "snappy"
        path += ".snappy"
        if @snappy_format == "file"
          output = compress_snappy_file(output)
        elsif
          output = compress_snappy_stream(output)
        end
      end
      write_data(path, output)
    end
  end

  def write_data(path, data)
    # Retry max_retry times. This can solve problems like leases being hold by another process. Sadly this is no
    # KNOWN_ERROR in rubys webhdfs client.
    write_tries = 0
    begin
      # Try to append to already existing file, which will work most of the times.
      @client.append(path, data)
      # File does not exist, so create it.
    rescue WebHDFS::FileNotFoundError
      # Add snappy header if format is "file".
      if @compression == "snappy" and @snappy_format == "file"
        @client.create(path, get_snappy_header! + data)
      elsif
        @client.create(path, data)
      end
      # Handle other write errors and retry to write max. @retry_times.
    rescue => e
      if write_tries < @retry_times
        @logger.warn("webhdfs write caused an exception: #{e.message}. Maybe you should increase retry_interval or reduce number of workers. Retrying...")
        sleep(@retry_interval * write_tries)
        write_tries += 1
        retry
      else
        # Issue error after max retries.
        @logger.error("Max write retries reached. Events will be discarded. Exception: #{e.message}")
      end
    end
  end

  def teardown
    buffer_flush(:final => true)
  end # def teardown
end # class LogStash::Outputs::WebHdfs
