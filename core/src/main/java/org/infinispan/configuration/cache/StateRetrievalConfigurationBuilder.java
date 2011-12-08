package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configures how state is retrieved when a new cache joins the cluster. Used with invalidation and
 * replication clustered modes.
 */
public class StateRetrievalConfigurationBuilder extends
      AbstractClusteringConfigurationChildBuilder<StateRetrievalConfiguration> {

   private static final Log log = LogFactory.getLog(StateRetrievalConfigurationBuilder.class);
   
   private boolean alwaysProvideInMemoryState = false;
   private Boolean fetchInMemoryState = null;
   private long initialRetryWaitTime = 500L;
   private long logFlushTimeout = TimeUnit.MINUTES.toMillis(1);
   private int maxNonProgressingLogWrites = 100;
   private int numRetries = 5;
   private int retryWaitTimeIncreaseFactor = 2;
   private long timeout = TimeUnit.MINUTES.toMillis(4);

   StateRetrievalConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * If true, allows the cache to provide in-memory state to a neighbor, even if the cache is not
    * configured to fetch state from its neighbors (fetchInMemoryState is false)
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
   public StateRetrievalConfigurationBuilder maxNonProgressingLogWrites(int i) {
      this.maxNonProgressingLogWrites = i;
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
      // certain combinations are illegal, such as state transfer + DIST
      if (fetchInMemoryState != null && fetchInMemoryState && getClusteringBuilder().cacheMode().isDistributed())
         throw new ConfigurationException(
               "Cache cannot use DISTRIBUTION mode and have fetchInMemoryState set to true.  Perhaps you meant to enable rehashing?");
   }

   @Override
   StateRetrievalConfiguration create() {

      // If replicated and fetch state transfer was not explicitly
      // disabled, then force enabling of state transfer
      if (getClusteringBuilder().cacheMode().isReplicated() && fetchInMemoryState == null) {
         log.debug("Cache is replicated but state transfer was not defined, so force enabling it");
         fetchInMemoryState(true);
      }
      if (fetchInMemoryState == null)
         fetchInMemoryState = false;
      return new StateRetrievalConfiguration(alwaysProvideInMemoryState, fetchInMemoryState.booleanValue(),
            initialRetryWaitTime, logFlushTimeout, maxNonProgressingLogWrites, numRetries, retryWaitTimeIncreaseFactor, timeout);
   }

}
