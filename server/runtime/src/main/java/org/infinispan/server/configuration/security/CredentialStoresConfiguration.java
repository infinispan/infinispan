package org.infinispan.server.configuration.security;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfiguration extends ConfigurationElement<CredentialStoresConfiguration> {
   private final Map<String, CredentialStoreConfiguration> credentialStores;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CredentialStoresConfiguration.class);
   }

   CredentialStoresConfiguration(AttributeSet attributes, Map<String, CredentialStoreConfiguration> credentialStores, Properties properties) {
      super(Element.CREDENTIAL_STORES, attributes);
      attributes.checkProtection();
      this.credentialStores = credentialStores;
      init(properties);
   }

   public Map<String, CredentialStoreConfiguration> credentialStores() {
      return credentialStores;
   }

   public char[] getCredential(String store, String alias) {
      return getCredential(store, alias, PasswordCredential.class).getPassword(ClearPassword.class).getPassword();
   }

   private <C extends Credential> C getCredential(String store, String alias, Class<C> type) {
      CredentialStoreConfiguration credentialStoreConfiguration;
      if (store == null) {
         if (credentialStores.size() == 1) {
            credentialStoreConfiguration = credentialStores.values().iterator().next();
         } else {
            throw Server.log.missingCredentialStoreName();
         }
      } else {
         credentialStoreConfiguration = credentialStores.get(store);
      }
      if (credentialStoreConfiguration == null) {
         throw Server.log.unknownCredentialStore(store);
      }
      C credential = credentialStoreConfiguration.getCredential(alias, type);
      if (credential == null) {
         throw Server.log.unknownCredential(alias, store);
      } else {
         return credential;
      }
   }

   private void init(Properties properties) {
      for(CredentialStoreConfiguration cs : credentialStores.values()) {
         cs.init(properties);
      }
   }
}
