require "logstash/namespace"

module LogStash
  module Outputs
    module WebHdfsHelper

      # Load a module
      # @param module_name [String] A module name
      # @raise [LoadError] If the module count not be loaded
      def load_module(module_name)
        begin
          require module_name
        rescue LoadError
          @logger.error("Module #{module_name} could not be loaded.")
          raise
        end
      end

      # Setup a WebHDFS client
      # @param host [String] The WebHDFS location
      # @param port [Number] The port used to do the communication
      # @param username [String] A valid HDFS user
      # @return [WebHDFS] An setup client instance
      def prepare_client(host, port, username)
        client = WebHDFS::Client.new(host, port, username)
        client.httpfs_mode = @use_httpfs
        client.open_timeout = @open_timeout
        client.read_timeout = @read_timeout
        client.retry_known_errors = @retry_known_errors
        client.retry_interval = @retry_interval if @retry_interval
        client.retry_times = @retry_times if @retry_times
        client
      end


      # Compress data using the gzip methods.
      # @param data [String] stream of data to be compressed
      # @return [String] the compressed stream of data
      def compress_gzip(data)
        buffer = StringIO.new('','w')
        compressor = Zlib::GzipWriter.new(buffer)
        begin
          compressor.write(data)
        ensure
          compressor.close()
        end
        buffer.string
      end

      # Compress snappy file.
      # @param data [binary] stream of data to be compressed
      # @return [String] the compressed stream of data
      def compress_snappy_file(data)
        # Encode data to ASCII_8BIT (binary)
        data= data.encode(Encoding::ASCII_8BIT, "binary", :undef => :replace)
        buffer = StringIO.new('', 'w')
        buffer.set_encoding(Encoding::ASCII_8BIT)
        compressed = Snappy.deflate(data)
        buffer << [compressed.size, compressed].pack("Na*")
        buffer.string
      end

      def compress_snappy_stream(data)
        # Encode data to ASCII_8BIT (binary)
        data= data.encode(Encoding::ASCII_8BIT, "binary", :undef => :replace)
        buffer = StringIO.new
        buffer.set_encoding(Encoding::ASCII_8BIT)
        chunks = data.scan(/.{1,#{@snappy_bufsize}}/m)
        chunks.each do |chunk|
          compressed = Snappy.deflate(chunk)
          buffer << [chunk.size, compressed.size, compressed].pack("NNa*")
        end
        return buffer.string
      end

      def get_snappy_header!
        [MAGIC, DEFAULT_VERSION, MINIMUM_COMPATIBLE_VERSION].pack("a8NN")
      end

    end
  end
end
