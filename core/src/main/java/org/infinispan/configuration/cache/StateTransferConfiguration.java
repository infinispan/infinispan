package org.infinispan.configuration.cache;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with invalidation and replication clustered modes.
 * 
 * @since 5.1
 */
public class StateTransferConfiguration {

   private boolean fetchInMemoryState;
   private Boolean originalFetchInMemoryState;
   private long timeout;
   private int chunkSize;

   StateTransferConfiguration(boolean fetchInMemoryState, Boolean originalFetchInMemoryState, long timeout, int chunkSize) {
      this.fetchInMemoryState = fetchInMemoryState;
      this.originalFetchInMemoryState = originalFetchInMemoryState;
      this.timeout = timeout;
      this.chunkSize = chunkSize;
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * <p/>
    * In distributed mode, state is transferred between running caches as well, as the ownership of
    * keys changes (e.g. because a cache left the cluster). Disabling this setting means a key will
    * sometimes have less than {@code numOwner} owners.
    */
   public boolean fetchInMemoryState() {
      return fetchInMemoryState;
   }

   /**
    * We want to remember if the user didn't configure fetchInMemoryState for the default cache.
    */
   protected Boolean originalFetchInMemoryState() {
      return originalFetchInMemoryState;
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
   public StateTransferConfiguration timeout(long l) {
      timeout = l;
      return this;
   }

   /**
    * If &gt; 0, the state will be transferred in batches of {@code chunkSize} cache entries.
    * If &lt;= 0, the state will be transferred in all at once. Not recommended.
    */
   public int chunkSize() {
      return chunkSize;
   }

}
