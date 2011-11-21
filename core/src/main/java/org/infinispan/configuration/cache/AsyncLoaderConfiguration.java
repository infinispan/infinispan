package org.infinispan.configuration.cache;

public class AsyncLoaderConfiguration {
   
   private final boolean enabled;
   private final long flushLockTimeout;
   private final int modificationQueueSize;
   private final long shutdownTimeout;
   private final int threadPoolSize;
   
   AsyncLoaderConfiguration(boolean enabled, long flushLockTimeout, int modificationQueueSize, long shutdownTimeout,
         int threadPoolSize) {
      this.enabled = enabled;
      this.flushLockTimeout = flushLockTimeout;
      this.modificationQueueSize = modificationQueueSize;
      this.shutdownTimeout = shutdownTimeout;
      this.threadPoolSize = threadPoolSize;
   }

   public boolean enabled() {
      return enabled;
   }

   public long flushLockTimeout() {
      return flushLockTimeout;
   }

   public int modificationQueueSize() {
      return modificationQueueSize;
   }

   public long shutdownTimeout() {
      return shutdownTimeout;
   }

   public int threadPoolSize() {
      return threadPoolSize;
   }

}
