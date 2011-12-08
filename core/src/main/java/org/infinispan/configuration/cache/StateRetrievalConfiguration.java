package org.infinispan.configuration.cache;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with invalidation and replication clustered modes.
 */
public class StateRetrievalConfiguration {

   private final boolean alwaysProvideInMemoryState;
   private boolean fetchInMemoryState;
   private final long initialRetryWaitTime;
   private final long logFlushTimeout;
   private final int maxNonProgressingLogWrites;
   private final int numRetries;
   private final int retryWaitTimeIncreaseFactor;
   private long timeout;

   StateRetrievalConfiguration(boolean alwaysProvideInMemoryState, boolean fetchInMemoryState, long initialRetryWaitTime,
         long logFlushTimeout, int maxNonPorgressingLogWrites, int numRetries, int retryWaitTimeIncreaseFactory, long timeout) {
      this.alwaysProvideInMemoryState = alwaysProvideInMemoryState;
      this.fetchInMemoryState = fetchInMemoryState;
      this.initialRetryWaitTime = initialRetryWaitTime;
      this.logFlushTimeout = logFlushTimeout;
      this.maxNonProgressingLogWrites = maxNonPorgressingLogWrites;
      this.numRetries = numRetries;
      this.retryWaitTimeIncreaseFactor = retryWaitTimeIncreaseFactory;
      this.timeout = timeout;
   }

   /**
    * If true, this will allow the cache to provide in-memory state to a neighbor, even if the cache
    * is not configured to fetch state from its neighbors (fetchInMemoryState is false)
    */
   public boolean alwaysProvideInMemoryState() {
      return alwaysProvideInMemoryState;
   }

   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    */
   public boolean fetchInMemoryState() {
      return fetchInMemoryState;
   }
   
   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    */
   public StateRetrievalConfiguration fetchInMemoryState(boolean b) {
      this.fetchInMemoryState = b;
      return this;
   }

   /**
    * Initial wait time when backing off before retrying state transfer retrieval
    */
   public long initialRetryWaitTime() {
      return initialRetryWaitTime;
   }

   /**
    * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of
    * transaction logs.
    */
   public long logFlushTimeout() {
      return logFlushTimeout;
   }

   /**
    * This is the maximum number of non-progressing transaction log writes after which a
    * brute-force flush approach is resorted to, to synchronize transaction logs.
    */
   public int maxNonProgressingLogWrites() {
      return maxNonProgressingLogWrites;
   }

   /**
    * Number of state retrieval retries before giving up and aborting startup.
    */
   public int numRetries() {
      return numRetries;
   }

   /**
    * Wait time increase factor over successive state retrieval backoffs
    */
   public int retryWaitTimeIncreaseFactor() {
      return retryWaitTimeIncreaseFactor;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public long timeout() {
      return timeout;
   }
   
   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateRetrievalConfiguration timeout(long l) {
      this.timeout = l;
      return this;
   }

}
