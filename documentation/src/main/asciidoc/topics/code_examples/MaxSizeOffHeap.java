ConfigurationBuilder cfg = new ConfigurationBuilder();

cfg
  .memory()
    .storage(StorageType.OFF_HEAP)
    .maxSize("1.5GB")
    .whenFull(EvictionStrategy.REMOVE)
  .build());
