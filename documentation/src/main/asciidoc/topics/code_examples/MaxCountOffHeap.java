ConfigurationBuilder cfg = new ConfigurationBuilder();

cfg
  .encoding()
    .mediaType("application/x-protostream")
  .memory()
    .storage(StorageType.OFF_HEAP)
    .maxCount(500)
    .whenFull(EvictionStrategy.REMOVE)
  .build());
