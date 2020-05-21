Configuration c = new ConfigurationBuilder()
   .clustering().hash().groups().addGrouper(new KXGrouper())
   .build();
