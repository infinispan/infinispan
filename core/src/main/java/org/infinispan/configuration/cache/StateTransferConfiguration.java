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
   private boolean awaitInitialTransfer;
   private Boolean originalAwaitInitialTransfer;

   StateTransferConfiguration(boolean fetchInMemoryState, Boolean originalFetchInMemoryState, long timeout, int chunkSize,
                              boolean awaitInitialTransfer, Boolean originalAwaitInitialTransfer) {
      this.fetchInMemoryState = fetchInMemoryState;
      this.originalFetchInMemoryState = originalFetchInMemoryState;
      this.timeout = timeout;
      this.chunkSize = chunkSize;
      this.awaitInitialTransfer = awaitInitialTransfer;
      this.originalAwaitInitialTransfer = originalAwaitInitialTransfer;
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
    * The state will be transferred in batches of {@code chunkSize} cache entries.
    * If chunkSize is equal to Integer.MAX_VALUE, the state will be transferred in all at once. Not recommended.
    */
   public int chunkSize() {
      return chunkSize;
   }

   /**
    * If {@code true}, this will cause the first call to method {@code CacheManager.getCache()} on the joiner node to
    * block and wait until the joining is complete and the cache has finished receiving state from neighboring caches
    * (if fetchInMemoryState is enabled). This option applies to distributed and replicated caches only and is enabled
    * by default. Please note that setting this to {@code false} will make the cache object available immediately but
    * any access to keys that should be available locally but are not yet transferred will actually cause a (transparent)
    * remote access. While this will not have any impact on the logic of your application it might impact performance.
    */
   public boolean awaitInitialTransfer() {
      return awaitInitialTransfer;
   }

   /**
    * We want to remember if the user didn't configure awaitInitialTransfer for the default cache.
    */
   protected Boolean originalAwaitInitialTransfer() {
      return originalAwaitInitialTransfer;
   }

   @Override
   public String toString() {
      return "StateTransferConfiguration{" +
            "chunkSize=" + chunkSize +
            ", fetchInMemoryState=" + fetchInMemoryState +
            ", originalFetchInMemoryState=" + originalFetchInMemoryState +
            ", timeout=" + timeout +
            ", awaitInitialTransfer=" + awaitInitialTransfer +
            ", originalAwaitInitialTransfer=" + originalAwaitInitialTransfer +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StateTransferConfiguration that = (StateTransferConfiguration) o;

      if (chunkSize != that.chunkSize) return false;
      if (fetchInMemoryState != that.fetchInMemoryState) return false;
      if (timeout != that.timeout) return false;
      if (originalFetchInMemoryState != null ? !originalFetchInMemoryState.equals(that.originalFetchInMemoryState) : that.originalFetchInMemoryState != null)
         return false;
      if (awaitInitialTransfer != that.awaitInitialTransfer) return false;
      if (originalAwaitInitialTransfer != null ? !originalAwaitInitialTransfer.equals(that.originalAwaitInitialTransfer) : that.originalAwaitInitialTransfer != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (fetchInMemoryState ? 1 : 0);
      result = 31 * result + (originalFetchInMemoryState != null ? originalFetchInMemoryState.hashCode() : 0);
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + chunkSize;
      result = 31 * result + (awaitInitialTransfer ? 1 : 0);
      result = 31 * result + (originalAwaitInitialTransfer != null ? originalAwaitInitialTransfer.hashCode() : 0);
      return result;
   }

}
