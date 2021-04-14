package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.AUTH_DIGEST;
import static org.infinispan.persistence.remote.configuration.Element.AUTH_EXTERNAL;
import static org.infinispan.persistence.remote.configuration.Element.AUTH_PLAIN;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;

public class MechanismConfiguration {

   static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder("username", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder("password", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> REALM = AttributeDefinition.builder("realm", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SASL_MECHANISM = AttributeDefinition.builder("sasl-mechanism", null, String.class)
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

   static String serializeMechanism(String mechanism) {
      if (mechanism == null) return null;
      switch (mechanism) {
         case "PLAIN": {
            return AUTH_PLAIN.getLocalName();
         }
         case "DIGEST-MD5": {
            return AUTH_DIGEST.getLocalName();
         }
         case "EXTERNAL": {
            return AUTH_EXTERNAL.getLocalName();
         }
      }
      throw new CacheConfigurationException("Invalid sasl mechanism");
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MechanismConfiguration that = (MechanismConfiguration) o;

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
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
