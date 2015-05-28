Gem::Specification.new do |s|

  s.name            = 'logstash-output-webhdfs'
  s.version         = '0.0.1'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Plugin to write events to hdfs via webhdfs."
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Björn Puttmann, loshkovskyi"]
  s.email           = 'b.puttmann@dbap.de'
  s.homepage        = "http://www.dbap.de"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core', '>= 1.4.0', '< 2.0.0'
  s.add_runtime_dependency 'webhdfs'
  s.add_runtime_dependency 'snappy'
  s.add_development_dependency 'logstash-devutils'
end