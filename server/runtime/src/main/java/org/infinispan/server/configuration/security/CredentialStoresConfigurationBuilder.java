package org.infinispan.server.configuration.security;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.wildfly.security.credential.Credential;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfigurationBuilder implements Builder<CredentialStoresConfiguration> {

   private final AttributeSet attributes;
   private final Map<String, CredentialStoreConfigurationBuilder> credentialStores = new LinkedHashMap<>(2);

   public CredentialStoresConfigurationBuilder() {
      this.attributes = CredentialStoresConfiguration.attributeDefinitionSet();
   }

   public CredentialStoreConfigurationBuilder addCredentialStore(String name) {
      CredentialStoreConfigurationBuilder credentialStoreBuilder = new CredentialStoreConfigurationBuilder(this);
      credentialStores.put(name, credentialStoreBuilder);
      return credentialStoreBuilder;
   }

   public <C extends Credential> C getCredential(String store, String alias, Class<C> type) {
      CredentialStoreConfigurationBuilder credentialStoreConfigurationBuilder;
      if (store == null) {
         if (credentialStores.size() == 1) {
            credentialStoreConfigurationBuilder = credentialStores.values().iterator().next();
         } else {
            throw Server.log.missingCredentialStoreName();
         }
      } else {
         credentialStoreConfigurationBuilder = credentialStores.get(store);
      }
      if (credentialStoreConfigurationBuilder == null) {
         throw Server.log.unknownCredentialStore(store);
      }
      C credential = credentialStoreConfigurationBuilder.getCredential(alias, type);
      if (credential == null) {
         throw Server.log.unknownCredential(alias, store);
      } else {
         return credential;
      }
   }

   @Override
   public void validate() {
   }

   @Override
   public CredentialStoresConfiguration create() {
      List<CredentialStoreConfiguration> list = credentialStores.values().stream()
            .map(CredentialStoreConfigurationBuilder::create).collect(Collectors.toList());
      return new CredentialStoresConfiguration(attributes.protect(), list);
   }

   @Override
   public Builder<?> read(CredentialStoresConfiguration template) {
      return this;
   }
}
