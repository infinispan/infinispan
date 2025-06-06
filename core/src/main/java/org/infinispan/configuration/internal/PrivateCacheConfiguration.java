package org.infinispan.configuration.internal;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;

@SerializedWith(PrivateCacheConfigurationSerializer.class)
@BuiltBy(PrivateCacheConfigurationBuilder.class)
public class PrivateCacheConfiguration {
   public static final AttributeDefinition<ConsistentHashFactory> CONSISTENT_HASH_FACTORY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONSISTENT_HASH_FACTORY, null, ConsistentHashFactory.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   private final AttributeSet attributes;

   PrivateCacheConfiguration(AttributeSet attributeSet) {
      this.attributes = attributeSet.checkProtection();
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(PrivateCacheConfiguration.class, CONSISTENT_HASH_FACTORY);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ConsistentHashFactory<?> consistentHashFactory() {
      return attributes.attribute(CONSISTENT_HASH_FACTORY).get();
   }

   @Override
   public String toString() {
      return "PrivateGlobalConfiguration [attributes=" + attributes + ']';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PrivateCacheConfiguration that = (PrivateCacheConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
