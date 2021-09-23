ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().passivation(true)
       .addSoftIndexFileStore()
          .shared(false)
          .dataLocation("data")
          .indexLocation("index")
          .modificationQueueSize(2048);
