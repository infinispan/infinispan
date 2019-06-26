Configuration c = new ConfigurationBuilder()
               .memory().size(2000)
               .expiration().wakeUpInterval(5000l).lifespan(1000l).maxIdle(500l)
               .build();
