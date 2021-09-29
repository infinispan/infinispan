ConfigurationBuilder builder = new ConfigurationBuilder();
builder.encoding().mediaType("application/x-protostream")
       .memory()
         .maxSize("1.5GB")
         .whenFull(EvictionStrategy.REMOVE);
