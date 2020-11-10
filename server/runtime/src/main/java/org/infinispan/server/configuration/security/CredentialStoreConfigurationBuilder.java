package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.CREDENTIAL;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.RELATIVE_TO;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.TYPE;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.CredentialStoreSpi;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoreConfigurationBuilder implements Builder<CredentialStoreConfiguration> {
   private final AttributeSet attributes;
   private CredentialStoreSpi credentialStore;

   CredentialStoreConfigurationBuilder(CredentialStoresConfigurationBuilder credentialStoresConfigurationBuilder) {
      this.attributes = CredentialStoreConfiguration.attributeDefinitionSet();
   }

   public CredentialStoreConfigurationBuilder path(String value) {
      attributes.attribute(PATH).set(value);
      return this;
   }

   public CredentialStoreConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public CredentialStoreConfigurationBuilder type(String type) {
      attributes.attribute(TYPE).set(type);
      return this;
   }

   public CredentialStoreConfigurationBuilder credential(String credential) {
      attributes.attribute(CREDENTIAL).set(credential);
      return this;
   }

   @Override
   public void validate() {

   }

   @Override
   public CredentialStoreConfiguration create() {
      build();
      return new CredentialStoreConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(CredentialStoreConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   CredentialStoreSpi build() {
      if (credentialStore == null) {
         if (attributes.attribute(PATH).isNull()) {
            throw new IllegalStateException("file has to be specified");
         }
         String path = attributes.attribute(PATH).get();
         String relativeTo = attributes.attribute(RELATIVE_TO).get();
         String location = ParseUtils.resolvePath(path, relativeTo);
         credentialStore = new KeyStoreCredentialStore();
         final Map<String, String> map = new HashMap<>();
         map.put("location", location);
         map.put("create", "false");
         map.put("keyStoreType", attributes.attribute(TYPE).get());
         char[] credential = attributes.attribute(CREDENTIAL).get().toCharArray();
         try {
            credentialStore.initialize(
                  map,
                  new CredentialStore.CredentialSourceProtectionParameter(
                        IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential)))),
                  null
            );
         } catch (CredentialStoreException e) {
            // We ignore the exception if it's about automatic creation
            if (!e.getMessage().startsWith("ELY09518")) {
               throw new CacheConfigurationException(e);
            }
         }
      }
      return credentialStore;
   }

   public <C extends Credential> C getCredential(String alias, Class<C> type) {
      build();
      try {
         if (alias == null) {
            if (credentialStore.getAliases().size() == 1) {
               alias = credentialStore.getAliases().iterator().next();
            } else {
               throw Server.log.unspecifiedCredentialAlias();
            }
         }
         return credentialStore.retrieve(alias, type, null, null, null);
      } catch (CredentialStoreException e) {
         throw new CacheConfigurationException(e);
      }
   }
}
