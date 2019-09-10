GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().transport()
        .defaultTransport()
        .clusterName("prod-cluster")
         // Add custom JGroups stacks with the addProperty() method.
        .addProperty("configurationFile", "prod-jgroups-tcp.xml")
        .machineId("prod-machine").rackId("prod-rack")
        .build();
