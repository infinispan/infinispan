package org.infinispan.persistence.remote.configuration;

import static org.infinispan.commons.configuration.attributes.AttributeSerializer.SECRET;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;

public class MechanismConfiguration {

   static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder(Attribute.USERNAME, null, String.class).serializer(SECRET).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, String.class).serializer(SECRET).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> REALM = AttributeDefinition.builder(Attribute.REALM, null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SASL_MECHANISM = AttributeDefinition.builder(Attribute.SASL_MECHANISM, null, String.class)
         .immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MechanismConfiguration.class, USERNAME, PASSWORD, REALM, SASL_MECHANISM);
   }



   public MechanismConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public String username() {
      return attributes.attribute(USERNAME).get();
   }

   public char[] password() {
      return Util.toCharArray(attributes.attribute(PASSWORD).get());
   }

   public String realm() {
      return attributes.attribute(REALM).get();
   }

   public String saslMechanism() {
      return attributes.attribute(SASL_MECHANISM).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MechanismConfiguration that = (MechanismConfiguration) o;

      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "MechanismConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
