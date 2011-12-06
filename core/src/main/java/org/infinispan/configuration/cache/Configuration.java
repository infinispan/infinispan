package org.infinispan.configuration.cache;

public class Configuration {
   
   private final String name;
   private final ClassLoader classLoader; //TODO remove this
   private final ClusteringConfiguration clusteringConfiguration;
   private final CustomInterceptorsConfiguration customInterceptorsConfiguration;
   private final DataContainerConfiguration dataContainerConfiguration;
   private final DeadlockDetectionConfiguration deadlockDetectionConfiguration;
   private final EvictionConfiguration evictionConfiguration;
   private final ExpirationConfiguration expirationConfiguration;
   private final IndexingConfiguration indexingConfiguration;
   private final InvocationBatchingConfiguration invocationBatchingConfiguration;
   private final JMXStatisticsConfiguration jmxStatisticsConfiguration;
   private final LoadersConfiguration loadersConfiguration;
   private final LockingConfiguration lockingConfiguration;
   private final StoreAsBinaryConfiguration storeAsBinaryConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final VersioningConfiguration versioningConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;

   Configuration(String name, ClusteringConfiguration clusteringConfiguration,
         CustomInterceptorsConfiguration customInterceptorsConfiguration,
         DataContainerConfiguration dataContainerConfiguration, DeadlockDetectionConfiguration deadlockDetectionConfiguration,
         EvictionConfiguration evictionConfiguration, ExpirationConfiguration expirationConfiguration,
         IndexingConfiguration indexingConfiguration, InvocationBatchingConfiguration invocationBatchingConfiguration,
         JMXStatisticsConfiguration jmxStatisticsConfiguration,
         LoadersConfiguration loadersConfiguration,
         LockingConfiguration lockingConfiguration, StoreAsBinaryConfiguration storeAsBinaryConfiguration,
         TransactionConfiguration transactionConfiguration, UnsafeConfiguration unsafeConfiguration,
         VersioningConfiguration versioningConfiguration, ClassLoader cl) {
      this.name = name;
      this.clusteringConfiguration = clusteringConfiguration;
      this.customInterceptorsConfiguration = customInterceptorsConfiguration;
      this.dataContainerConfiguration = dataContainerConfiguration;
      this.deadlockDetectionConfiguration = deadlockDetectionConfiguration;
      this.evictionConfiguration = evictionConfiguration;
      this.expirationConfiguration = expirationConfiguration;
      this.indexingConfiguration = indexingConfiguration;
      this.invocationBatchingConfiguration = invocationBatchingConfiguration;
      this.jmxStatisticsConfiguration = jmxStatisticsConfiguration;
      this.loadersConfiguration = loadersConfiguration;
      this.lockingConfiguration = lockingConfiguration;
      this.storeAsBinaryConfiguration = storeAsBinaryConfiguration;
      this.transactionConfiguration = transactionConfiguration;
      this.unsafeConfiguration = unsafeConfiguration;
      this.versioningConfiguration = versioningConfiguration;
      this.classLoader = cl;
   }

   public String name() {
      return name;
   }
   
   /**
    * Will be removed with no replacement
    * @return
    */
   @Deprecated
   public ClassLoader classLoader() {
      return classLoader;
   }
   
   public ClusteringConfiguration clustering() {
      return clusteringConfiguration;
   }
   
   public CustomInterceptorsConfiguration customInterceptors() {
      return customInterceptorsConfiguration;
   }
   
   public DataContainerConfiguration dataContainer() {
      return dataContainerConfiguration;
   }
   
   public DeadlockDetectionConfiguration deadlockDetection() {
      return deadlockDetectionConfiguration;
   }
   
   public EvictionConfiguration eviction() {
      return evictionConfiguration;
   }
   
   public ExpirationConfiguration expiration() {
      return expirationConfiguration;
   }
   
   public IndexingConfiguration indexing() {
      return indexingConfiguration;
   }
   
   public InvocationBatchingConfiguration invocationBatching() {
      return invocationBatchingConfiguration;
   }
   
   public JMXStatisticsConfiguration jmxStatistics() {
      return jmxStatisticsConfiguration;
   }
   
   public LoadersConfiguration loaders() {
      return loadersConfiguration;
   }
   
   public LockingConfiguration locking() {
      return lockingConfiguration;
   }
   
   public StoreAsBinaryConfiguration storeAsBinary() {
      return storeAsBinaryConfiguration;
   }
   
   public TransactionConfiguration transaction() {
      return transactionConfiguration;
   }
   
   public UnsafeConfiguration unsafe() {
      return unsafeConfiguration;
   }

   public VersioningConfiguration versioningConfiguration() {
      return versioningConfiguration;
   }

   public boolean stateTransferEnabled() {
      return clustering().stateRetrieval().fetchInMemoryState() || loaders().fetchPersistentState();
   }

   @Deprecated
   public boolean onePhaseCommit() {
      return clusteringConfiguration.cacheMode().isSynchronous();
   }
}
