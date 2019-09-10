GlobalConfiguration gc = new GlobalConfigurationBuilder()
   .transport().defaultTransport()
   .addProperty("configurationFile", "/default-configs/default-jgroups-tcp.xml")
   .build();
