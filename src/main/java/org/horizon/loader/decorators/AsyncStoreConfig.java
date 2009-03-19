package org.horizon.loader.decorators;

import org.horizon.config.AbstractNamedCacheConfigurationBean;

/**
 * Configuration for the async cache loader
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class AsyncStoreConfig extends AbstractNamedCacheConfigurationBean {
   boolean enabled;
   int batchSize = 100;
   long pollWait = 100;
   int queueSize = 10000;
   int threadPoolSize = 1;

   public boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(boolean enabled) {
      testImmutability("enabled");
      this.enabled = enabled;
   }

   public int getBatchSize() {
      return batchSize;
   }

   public void setBatchSize(int batchSize) {
      testImmutability("batchSize");
      this.batchSize = batchSize;
   }

   public long getPollWait() {
      return pollWait;
   }

   public void setPollWait(long pollWait) {
      testImmutability("pollWait");
      this.pollWait = pollWait;
   }

   public int getQueueSize() {
      return queueSize;
   }

   public void setQueueSize(int queueSize) {
      testImmutability("queueSize");
      this.queueSize = queueSize;
   }

   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   public void setThreadPoolSize(int threadPoolSize) {
      testImmutability("threadPoolSize");
      this.threadPoolSize = threadPoolSize;
   }

   @Override
   public AsyncStoreConfig clone() {
      try {
         return (AsyncStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
   }

}
