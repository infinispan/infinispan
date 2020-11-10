package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfiguration implements ConfigurationInfo {
   private static final ElementDefinition<CredentialStoresConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(Element.CREDENTIAL_STORES.toString());

   private final List<ConfigurationInfo> configs = new ArrayList<>();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CredentialStoresConfiguration.class);
   }

   private final AttributeSet attributes;

   CredentialStoresConfiguration(AttributeSet attributes, List<CredentialStoreConfiguration> credentialStores) {
      this.attributes = attributes.checkProtection();
      this.configs.addAll(credentialStores);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configs;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition<CredentialStoresConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CredentialStoresConfiguration that = (CredentialStoresConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "CredentialStoresConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
