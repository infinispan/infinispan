package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 *
 * @deprecated Since 9.0, the {@code replTimeout} attribute is now {@code ClusteringConfigurationBuilder.remoteTimeout}.
 */
@Deprecated
public class SyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<SyncConfiguration> {
   private final AttributeSet attributes;

   protected SyncConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.attributes = SyncConfiguration.attributeDefinitionSet();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    *
    * @deprecated Since 9.0, please use {@link ClusteringConfigurationBuilder#remoteTimeout(long)}.
    */
   @Deprecated
   public SyncConfigurationBuilder replTimeout(long l) {
      attributes.attribute(SyncConfiguration.REPL_TIMEOUT).set(l);
      return this;
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    *
    * @deprecated Since 9.0, please use {@link ClusteringConfigurationBuilder#remoteTimeout(long, TimeUnit)}.
    */
   @Deprecated
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
      return new SyncConfiguration(attributes.protect());
   }

   @Override
   public SyncConfigurationBuilder read(SyncConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "SyncConfigurationBuilder [attributes=" + attributes + "]";
   }
}
