ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().passivation(true)
       .addStore(SingleFileStoreConfigurationBuilder.class)
          .shared(false)
          .preload(true)
          .fetchPersistentState(true);
