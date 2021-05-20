File file = new File("path/to/infinispan.xml")
ConfigurationBuilder builder = new ConfigurationBuilder();
builder.remoteCache("another-cache")
       .configuration("<distributed-cache name=\"another-cache\"/>");
builder.remoteCache("my.other.cache")
       .configurationURI(file.toURI());
