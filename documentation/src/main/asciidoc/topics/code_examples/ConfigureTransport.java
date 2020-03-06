GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().transport()
        .defaultTransport()
        .clusterName("prod-cluster")
        .addProperty("configurationFile", "prod-jgroups-tcp.xml") <1>
        .build();
