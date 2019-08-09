package org.infinispan.server.core.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class EncryptionConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("securityRealm", null, String.class).build();
   static final AttributeDefinition<Boolean> REQUIRE_CLIENT_AUTH = AttributeDefinition.builder("requireSslClientAuth", null, Boolean.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("encryption");
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EncryptionConfiguration.class, REQUIRE_CLIENT_AUTH, SECURITY_REALM);
   }

   private final AttributeSet attributes;

   private final List<SniConfiguration> sniConfigurations;

   EncryptionConfiguration(AttributeSet attributes, List<SniConfiguration> sniConfigurations) {
      this.attributes = attributes.checkProtection();
      this.sniConfigurations = sniConfigurations;
      this.subElements.addAll(sniConfigurations);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   List<SniConfiguration> sniConfigurations() {
      return sniConfigurations;
   }

   public String realm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean requireClientAuth() {
      return attributes.attribute(REQUIRE_CLIENT_AUTH).get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EncryptionConfiguration that = (EncryptionConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      return sniConfigurations.equals(that.sniConfigurations);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + sniConfigurations.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "EncryptionConfiguration{" +
            "attributes=" + attributes +
            ", sniConfigurations=" + sniConfigurations +
            '}';
   }
}
