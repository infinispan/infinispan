Configuration c = new ConfigurationBuilder()
   .clustering().hash().groups().enabled()
   .build();
