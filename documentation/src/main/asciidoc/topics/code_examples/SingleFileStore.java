ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence()
      .passivation(false)
      .addSingleFileStore()
         .preload(true)
         .shared(false)
         .location(System.getProperty("java.io.tmpdir"))
         .async()
            .enabled(true)
