ConfigurationBuilder cfg = new ConfigurationBuilder();

cfg
  .encoding()
    .mediaType("application/x-protostream")
  .memory()
    .maxSize("1.5GB")
    .whenFull(EvictionStrategy.REMOVE)
  .build());
