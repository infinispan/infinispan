package org.infinispan.cloudevents.configuration;

import static org.infinispan.cloudevents.impl.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * A {@link Builder} implementation of {@link CloudEventsGlobalConfiguration}.
 *
 * @author Dan Berindei
 * @see CloudEventsGlobalConfiguration
 * @since 12
 */
@Experimental
public class CloudEventsGlobalConfigurationBuilder implements Builder<CloudEventsGlobalConfiguration> {

   private final AttributeSet attributes;

   public CloudEventsGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.attributes = CloudEventsGlobalConfiguration.attributeSet();
   }

   public CloudEventsGlobalConfigurationBuilder bootstrapServers(String bootstrapServers) {
      this.attributes.attribute(CloudEventsGlobalConfiguration.BOOTSTRAP_SERVERS).set(bootstrapServers);
      return this;
   }

   public CloudEventsGlobalConfigurationBuilder acks(String acks) {
      this.attributes.attribute(CloudEventsGlobalConfiguration.ACKS).set(acks);
      return this;
   }

   public CloudEventsGlobalConfigurationBuilder auditTopic(String topic) {
      this.attributes.attribute(CloudEventsGlobalConfiguration.AUDIT_TOPIC).set(topic);
      return this;
   }

   public CloudEventsGlobalConfigurationBuilder cacheEntriesTopic(String topic) {
      this.attributes.attribute(CloudEventsGlobalConfiguration.CACHE_ENTRIES_TOPIC).set(topic);
      return this;
   }

   @Override
   public void validate() {
      String bootstrapServers = attributes.attribute(CloudEventsGlobalConfiguration.BOOTSTRAP_SERVERS).get();
      String cacheEntriesTopic = attributes.attribute(CloudEventsGlobalConfiguration.CACHE_ENTRIES_TOPIC).get();
      String auditTopic = attributes.attribute(CloudEventsGlobalConfiguration.AUDIT_TOPIC).get();
      if (!cacheEntriesTopic.isEmpty() || !auditTopic.isEmpty()) {
         if (bootstrapServers.isEmpty()) {
            throw CONFIG.bootstrapServersRequired();
         }
      }
   }

   @Override
   public CloudEventsGlobalConfiguration create() {
      return new CloudEventsGlobalConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(CloudEventsGlobalConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "CloudEventsGlobalConfigurationBuilder [attributes=" + attributes + ']';
   }
}
