logstash-webhdfs
================

A logstash plugin to store events via webhdfs.

Tested with v1.3.3, v1.4.0 and 1.5.0.

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

This plugin only has a mandatory dependency on the webhdfs gem from Kazuki Ohta and TAGOMORI Satoshi (@see: https://github.com/kzk/webhdfs). Optional dependencies are zlib and snappy gem.

No jars from hadoop are needed, thus reducing configuration and compatibility problems.

## Installation
Change into your logstash install directory and execute:
```
bin/plugin install logstash-output-webhdfs
```

## Documentation

Example configuration:

    output {
        webhdfs {
            workers => 2
            server => "your.nameno.de:14000"
            user => "flume"
            path => "/user/flume/logstash/dt=%{+Y}-%{+M}-%{+d}/logstash-%{+H}.log"
            flush_size => 500
            compression => "snappy"
            idle_flush_time => 10
            retry_interval => 0.5
        }
    }

For a complete list of options, see config section in source code.

This plugin has dependencies on:
 * webhdfs module @<https://github.com/kzk/webhdfs>
 * snappy module @<https://github.com/miyucy/snappy>