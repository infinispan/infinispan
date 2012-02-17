package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 */
public class SyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<SyncConfiguration> {

   private long replTimeout = TimeUnit.SECONDS.toMillis(15);
   
   protected SyncConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public SyncConfigurationBuilder replTimeout(long l) {
      this.replTimeout = l;
      return this;
   }

   @Override
   void validate() {
      
   }

   @Override
   SyncConfiguration create() {
      return new SyncConfiguration(replTimeout);
   }
   
   @Override
   public SyncConfigurationBuilder read(SyncConfiguration template) {
      this.replTimeout = template.replTimeout();
      return this;
   }

   @Override
   public String toString() {
      return "SyncConfigurationBuilder{" +
            "replTimeout=" + replTimeout +
            '}';
   }

}
