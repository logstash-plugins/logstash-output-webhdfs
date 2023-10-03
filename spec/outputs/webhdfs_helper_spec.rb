# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/webhdfs'
require 'webhdfs'
require 'logstash-output-webhdfs_jars'


describe "webhdfs helpers" do

  let(:host) { 'localhost' }
  let(:user) { 'hadoop' }
  let(:path) { '/test.log' }

  let(:config) { { 'host' =>host, 'user' => user, 'path' => path, 'compression' => 'none' } }

  let(:sample_data) { "Something very very very long to compress" }

  subject(:plugin) { LogStash::Plugin.lookup("output", "webhdfs").new(config) }

  context "when compressing using vendor snappy" do
    it "should return a valid byte array" do
      compressed = subject.compress_snappy_file(sample_data)

      expect(compressed).not_to be(:nil)
    end

    it "should contains all the data" do
      compressed = subject.compress_snappy_file(sample_data)

      #remove the length integer (32 bit) added by compress_snappy_file, 4 bytes, from compressed
      uncompressed = subject.snappy_inflate(compressed[4..-1])

      expect(uncompressed).to eq(sample_data)
    end
  end
end

