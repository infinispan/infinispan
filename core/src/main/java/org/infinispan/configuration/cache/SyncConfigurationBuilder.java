package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 */
public class SyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<SyncConfiguration> {

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

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public SyncConfigurationBuilder replTimeout(long l, TimeUnit unit) {
      return replTimeout(unit.toMillis(l));
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public SyncConfiguration create() {
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
