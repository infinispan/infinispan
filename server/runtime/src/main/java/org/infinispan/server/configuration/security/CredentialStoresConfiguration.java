package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Element;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfiguration extends ConfigurationElement<CredentialStoresConfiguration> {
   private final AttributeSet attributes;
   private final List<CredentialStoreConfiguration> credentialStores;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CredentialStoresConfiguration.class);
   }

   CredentialStoresConfiguration(AttributeSet attributes, List<CredentialStoreConfiguration> credentialStores) {
      super(Element.CREDENTIAL_STORES, attributes);
      this.attributes = attributes.checkProtection();
      this.credentialStores = credentialStores;
   }

   public List<CredentialStoreConfiguration> credentialStores() {
      return credentialStores;
   }
}
