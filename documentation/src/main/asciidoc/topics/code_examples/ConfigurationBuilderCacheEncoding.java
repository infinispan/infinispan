//Create cache configuration that encodes keys and values as ProtoStream
ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC)
       .encoding().key().mediaType("application/x-protostream")
       .encoding().value().mediaType("application/x-protostream");
