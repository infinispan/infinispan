ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().passivation(true)
       .addSoftIndexFileStore()
          .dataLocation("data")
          .indexLocation("index")
          .modificationQueueSize(2048);
