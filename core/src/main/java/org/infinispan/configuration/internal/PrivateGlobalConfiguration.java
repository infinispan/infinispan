package org.infinispan.configuration.internal;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * An internal configuration.
 * <p>
 * This is an internal configuration to be used by Infinispan modules when some advanced or ergonomic configuration is
 * needed.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@SerializedWith(PrivateGlobalConfigurationSerializer.class)
@BuiltBy(PrivateGlobalConfigurationBuilder.class)
public class PrivateGlobalConfiguration {

   static final AttributeDefinition<Boolean> SERVER_MODE = AttributeDefinition.builder("server-mode", false).immutable().build();
   private final AttributeSet attributes;

   PrivateGlobalConfiguration(AttributeSet attributeSet) {
      this.attributes = attributeSet.checkProtection();
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(PrivateGlobalConfiguration.class, SERVER_MODE);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean isServerMode() {
      return attributes.attribute(SERVER_MODE).get();
   }

   @Override
   public String toString() {
      return "PrivateGlobalConfiguration [attributes=" + attributes + ']';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PrivateGlobalConfiguration that = (PrivateGlobalConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
