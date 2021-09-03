ClusterExecutor clusterExecutor = cacheManager.executor();
clusterExecutor.singleNodeSubmission().filterTargets(policy);
for (int i = 0; i < invocations; ++i) {
   clusterExecutor.submitConsumer((cacheManager) -> {
      TransportConfiguration tc =
      cacheManager.getCacheManagerConfiguration().transport();
      return tc.siteId() + tc.rackId() + tc.machineId();
   }, triConsumer).get(10, TimeUnit.SECONDS);
}
