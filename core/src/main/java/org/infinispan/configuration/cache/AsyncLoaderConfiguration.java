package org.infinispan.configuration.cache;

public class AsyncLoaderConfiguration {
   
   private final boolean enabled;
   private long flushLockTimeout;
   private final int modificationQueueSize;
   private long shutdownTimeout;
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
   
   public AsyncLoaderConfiguration flushLockTimeout(long l) {
      this.flushLockTimeout = l;
      return this;
   }

   public int modificationQueueSize() {
      return modificationQueueSize;
   }

   public long shutdownTimeout() {
      return shutdownTimeout;
   }
   
   public AsyncLoaderConfiguration shutdownTimeout(long l) {
      this.shutdownTimeout = l;
      return this;
   }

   public int threadPoolSize() {
      return threadPoolSize;
   }

}
