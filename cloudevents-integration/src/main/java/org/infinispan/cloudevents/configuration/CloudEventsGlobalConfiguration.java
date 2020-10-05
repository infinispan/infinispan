package org.infinispan.cloudevents.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * Cloud events integration configuration.
 * <p>
 *
 * @author Dan Berindei
 * @since 12
 */
@Experimental
@SerializedWith(CloudEventsGlobalConfigurationSerializer.class)
@BuiltBy(CloudEventsGlobalConfigurationBuilder.class)
public class CloudEventsGlobalConfiguration {

   static final AttributeDefinition<String> BOOTSTRAP_SERVERS =
         AttributeDefinition.builder("bootstrap-servers", "").immutable().build();
   static final AttributeDefinition<String> ACKS =
         AttributeDefinition.builder("acks", "all").immutable().build();
   static final AttributeDefinition<String> CACHE_ENTRIES_TOPIC =
         AttributeDefinition.builder("cache-entries-topic", "").immutable().build();

   static final AttributeDefinition<String> AUDIT_TOPIC =
         AttributeDefinition.builder("audit-topic", "").immutable().build();
   private final AttributeSet attributes;

   CloudEventsGlobalConfiguration(AttributeSet attributeSet) {
      this.attributes = attributeSet.checkProtection();
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(CloudEventsGlobalConfiguration.class, BOOTSTRAP_SERVERS, ACKS, AUDIT_TOPIC, CACHE_ENTRIES_TOPIC);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String bootstrapServers() {
      return attributes.attribute(BOOTSTRAP_SERVERS).get();
   }

   public String acks() {
      return attributes.attribute(ACKS).get();
   }

   public String auditTopic() {
      return attributes.attribute(AUDIT_TOPIC).get();
   }

   public String cacheEntriesTopic() {
      return attributes.attribute(CACHE_ENTRIES_TOPIC).get();
   }

   @Override
   public String toString() {
      return "CloudEventsGlobalConfiguration [attributes=" + attributes + ']';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CloudEventsGlobalConfiguration that = (CloudEventsGlobalConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   public boolean auditEventsEnabled() {
      return !auditTopic().isEmpty();
   }

   public boolean cacheEntryEventsEnabled() {
      return !cacheEntriesTopic().isEmpty();
   }
}
