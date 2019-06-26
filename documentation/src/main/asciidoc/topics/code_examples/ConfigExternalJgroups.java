GlobalConfiguration gc = new GlobalConfigurationBuilder()
   .transport().defaultTransport()
   .addProperty("configurationFile", "jgroups.xml")
   .build();
