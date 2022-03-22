package org.infinispan.server.configuration.security;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.PasswordCredentialSource;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.source.CredentialSource;
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

   public CredentialSource getCredentialSource(String store, String alias) {
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
      PasswordCredential credential = credentialStoreConfiguration.getCredential(alias, PasswordCredential.class);
      if (credential == null) {
         throw Server.log.unknownCredential(alias, store);
      } else {
         return new PasswordCredentialSource(credential);
      }
   }

   private void init(Properties properties) {
      for (CredentialStoreConfiguration cs : credentialStores.values()) {
         cs.init(properties);
      }
   }

   public static char[] resolvePassword(org.infinispan.commons.configuration.attributes.Attribute<Supplier<CredentialSource>> attribute) {
      return attribute.isNull() ? null : resolvePassword(attribute.get());
   }

   public static char[] resolvePassword(Supplier<CredentialSource> supplier) {
      try {
         CredentialSource credentialSource = supplier.get();
         return credentialSource.getCredential(PasswordCredential.class).getPassword(ClearPassword.class).getPassword();
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }
}
