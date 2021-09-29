GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder()
  .transport()
  .clusterName("MyCluster")
  .machineId("LinuxServer01")
  .rackId("Rack01")
  .siteId("US-WestCoast");
