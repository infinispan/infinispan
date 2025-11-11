// mode=local,language=javascript,parameters=[k]
var topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
topology.getDistribution(k).isPrimary()
