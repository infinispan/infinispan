ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence()
       .simpleConnection()
         .connectionUrl("jdbc:h2://localhost")
         .driverClass("org.h2.Driver")
         .username("admin")
         .password("changeme");
