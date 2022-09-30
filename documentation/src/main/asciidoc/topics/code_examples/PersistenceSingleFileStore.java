ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().passivation(true)
       .addStore(SingleFileStoreConfigurationBuilder.class)
          .preload(true);
