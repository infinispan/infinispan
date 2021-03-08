//Create cache configuration that encodes keys and values as Protobuf
ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC);
builder.encoding().key().mediaType("application/x-protostream");
builder.encoding().value().mediaType("application/x-protostream");
