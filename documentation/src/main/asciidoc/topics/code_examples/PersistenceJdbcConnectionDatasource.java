ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence()
       .dataSource()
         .jndiUrl("java:/StringStoreWithManagedConnectionTest/DS");
