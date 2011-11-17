package org.infinispan.configuration.cache;

/**
 * Configures how state is retrieved when a new cache joins the cluster. Used with invalidation and
 * replication clustered modes.
 */
public class StateRetrievalConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<StateRetrievalConfiguration> {
   
   private boolean alwaysProvideInMemoryState;
   private boolean fetchInMemoryState;
   private long initialRetryWaitTime;
   private long logFlushTimeout;
   private int maxNonPorgressingLogWrites;
   private int numRetries;
   private int retryWaitTimeIncreaseFactor;
   private long timeout;
   
   StateRetrievalConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * If true, allows the cache to provide in-memory state to a neighbor, even if the cache is not configured
    * to fetch state from its neighbors (fetchInMemoryState is false)
    */
   public StateRetrievalConfigurationBuilder alwaysProvideInMemoryState(boolean b) {
      this.alwaysProvideInMemoryState = b;
      return this;
   }

   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    */
   public StateRetrievalConfigurationBuilder fetchInMemoryState(boolean b) {
      this.fetchInMemoryState = b;
      return this;
   }

   /**
    * Initial wait time when backing off before retrying state transfer retrieval
    */
   public StateRetrievalConfigurationBuilder initialRetryWaitTime(long l) {
      this.initialRetryWaitTime = l;
      return this;
   }

   /**
    * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of
    * transaction logs.
    */
   public StateRetrievalConfigurationBuilder logFlushTimeout(long l) {
      this.logFlushTimeout = l;
      return this;
   }

   /**
    * This is the maximum number of non-progressing transaction log writes after which a brute-force
    * flush approach is resorted to, to synchronize transaction logs.
    */
   public StateRetrievalConfigurationBuilder maxNonPorgressingLogWrites(int i) {
      this.maxNonPorgressingLogWrites = i;
      return this;
   }

   /**
    * Number of state retrieval retries before giving up and aborting startup.
    */
   public StateRetrievalConfigurationBuilder numRetries(int i) {
      this.numRetries = i;
      return this;
   }

   /**
    * Wait time increase factor over successive state retrieval backoffs
    */
   public StateRetrievalConfigurationBuilder retryWaitTimeIncreaseFactor(int i) {
      this.retryWaitTimeIncreaseFactor = i;
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateRetrievalConfigurationBuilder timeout(long l) {
      this.timeout = l;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   StateRetrievalConfiguration create() {
      return new StateRetrievalConfiguration(alwaysProvideInMemoryState, fetchInMemoryState, initialRetryWaitTime, logFlushTimeout, maxNonPorgressingLogWrites, numRetries, retryWaitTimeIncreaseFactor, timeout);
   }

}
