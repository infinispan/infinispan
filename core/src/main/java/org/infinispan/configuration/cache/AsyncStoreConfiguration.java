package org.infinispan.configuration.cache;

/**
 * Configuration for the async cache store. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfiguration {

   private final boolean enabled;
   private long flushLockTimeout;
   private final int modificationQueueSize;
   private long shutdownTimeout;
   private final int threadPoolSize;

   AsyncStoreConfiguration(boolean enabled, long flushLockTimeout, int modificationQueueSize, long shutdownTimeout,
                           int threadPoolSize) {
      this.enabled = enabled;
      this.flushLockTimeout = flushLockTimeout;
      this.modificationQueueSize = modificationQueueSize;
      this.shutdownTimeout = shutdownTimeout;
      this.threadPoolSize = threadPoolSize;
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    *
    * @return
    */
   public long flushLockTimeout() {
      return flushLockTimeout;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncStoreConfiguration flushLockTimeout(long l) {
      this.flushLockTimeout = l;
      return this;
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public int modificationQueueSize() {
      return modificationQueueSize;
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public long shutdownTimeout() {
      return shutdownTimeout;
   }

   public AsyncStoreConfiguration shutdownTimeout(long l) {
      this.shutdownTimeout = l;
      return this;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public int threadPoolSize() {
      return threadPoolSize;
   }

   @Override
   public String toString() {
      return "AsyncStoreConfiguration{" +
            "enabled=" + enabled +
            ", flushLockTimeout=" + flushLockTimeout +
            ", modificationQueueSize=" + modificationQueueSize +
            ", shutdownTimeout=" + shutdownTimeout +
            ", threadPoolSize=" + threadPoolSize +
            '}';
   }

}
