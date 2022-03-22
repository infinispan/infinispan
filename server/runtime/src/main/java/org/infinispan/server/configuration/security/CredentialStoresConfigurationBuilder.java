package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfigurationBuilder implements Builder<CredentialStoresConfiguration> {

   private final AttributeSet attributes;
   private final Map<String, CredentialStoreConfigurationBuilder> credentialStores = new LinkedHashMap<>(2);
   private final List<CredentialStoreSourceSupplier> suppliers = new ArrayList<>();
   private final ServerConfigurationBuilder builder;

   public CredentialStoresConfigurationBuilder(ServerConfigurationBuilder builder) {
      this.builder = builder;
      this.attributes = CredentialStoresConfiguration.attributeDefinitionSet();
   }

   public CredentialStoreConfigurationBuilder addCredentialStore(String name) {
      CredentialStoreConfigurationBuilder credentialStoreBuilder = new CredentialStoreConfigurationBuilder(name);
      credentialStores.put(name, credentialStoreBuilder);
      return credentialStoreBuilder;
   }

   @Override
   public CredentialStoresConfiguration create() {
      Map<String, CredentialStoreConfiguration> map = credentialStores.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().create()));
      CredentialStoresConfiguration configuration = new CredentialStoresConfiguration(attributes.protect(), map, builder.properties());
      for (CredentialStoreSourceSupplier s : suppliers) {
         s.configuration = configuration;
      }
      return configuration;
   }

   @Override
   public Builder<?> read(CredentialStoresConfiguration template) {
      credentialStores.clear();
      template.credentialStores().forEach((k, v) -> addCredentialStore(k).read(v));
      return this;
   }

   public Supplier<CredentialSource> getCredential(String store, String alias) {
      CredentialStoreSourceSupplier credentialSupplier = new CredentialStoreSourceSupplier(store, alias);
      suppliers.add(credentialSupplier);
      return credentialSupplier;
   }

   public static class CredentialStoreSourceSupplier implements Supplier<CredentialSource> {
      final String store;
      final String alias;
      CredentialStoresConfiguration configuration;

      CredentialStoreSourceSupplier(String store, String alias) {
         this.store = store;
         this.alias = alias;
      }

      @Override
      public CredentialSource get() {
         return configuration.getCredentialSource(store, alias);
      }

      public String getStore() {
         return store;
      }

      public String getAlias() {
         return alias;
      }
   }
}
