ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence()
       .connectionPool()
         .connectionUrl("jdbc:h2:mem:infinispan_string_based;DB_CLOSE_DELAY=-1")
         .username("sa")
         .driverClass("org.h2.Driver");
