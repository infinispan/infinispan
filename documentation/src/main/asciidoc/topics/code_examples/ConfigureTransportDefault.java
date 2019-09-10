GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().transport()
        .defaultTransport()
        .clusterName("qa-cluster")
         // Use default JGroups stacks with the addProperty() method.
        .addProperty("configurationFile", "default-jgroups-tcp.xml")
        .machineId("qa-machine").rackId("qa-rack")
        .build();
