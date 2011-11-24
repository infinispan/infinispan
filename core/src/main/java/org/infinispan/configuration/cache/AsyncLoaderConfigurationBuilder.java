package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

public class AsyncLoaderConfigurationBuilder extends AbstractLoaderConfigurationChildBuilder<AsyncLoaderConfiguration> {

   private boolean enabled = false;
   private long flushLockTimeout = 1;
   private int modificationQueueSize = 1024;
   private long shutdownTimeout = TimeUnit.SECONDS.toMillis(25);
   private int threadPoolSize = 1;
   
   AsyncLoaderConfigurationBuilder(LoaderConfigurationBuilder builder) {
      super(builder);
   }

   public AsyncLoaderConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public AsyncLoaderConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public AsyncLoaderConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public AsyncLoaderConfigurationBuilder flushLockTimeout(long l) {
      this.flushLockTimeout = l;
      return this;
   }

   public AsyncLoaderConfigurationBuilder modificationQueueSize(int i) {
      this.modificationQueueSize = i;
      return this;
   }

   public AsyncLoaderConfigurationBuilder shutdownTimeout(long l) {
      this.shutdownTimeout = l;
      return this;
   }

   public AsyncLoaderConfigurationBuilder threadPoolSize(int i) {
      this.threadPoolSize = i;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   AsyncLoaderConfiguration create() {
      return new AsyncLoaderConfiguration(enabled, flushLockTimeout, modificationQueueSize, shutdownTimeout, threadPoolSize);
   }

}
