ConfigurationBuilder cfg = new ConfigurationBuilder();

cfg
  .encoding()
    .mediaType("application/x-protostream")
  .memory()
    .maxCount(500)
    .whenFull(EvictionStrategy.REMOVE)
  .build());
