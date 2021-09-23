ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence()
       .async()
       .modificationQueueSize(2048)
       .failSilently(true);
