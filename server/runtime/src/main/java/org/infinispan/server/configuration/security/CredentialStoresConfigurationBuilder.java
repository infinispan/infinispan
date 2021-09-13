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

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoresConfigurationBuilder implements Builder<CredentialStoresConfiguration> {

   private final AttributeSet attributes;
   private final Map<String, CredentialStoreConfigurationBuilder> credentialStores = new LinkedHashMap<>(2);
   private final List<CredentialSupplier> suppliers = new ArrayList<>();
   private final ServerConfigurationBuilder builder;

   public CredentialStoresConfigurationBuilder(ServerConfigurationBuilder builder) {
      this.builder = builder;
      this.attributes = CredentialStoresConfiguration.attributeDefinitionSet();
   }

   public CredentialStoreConfigurationBuilder addCredentialStore(String name) {
      CredentialStoreConfigurationBuilder credentialStoreBuilder = new CredentialStoreConfigurationBuilder(this, name);
      credentialStores.put(name, credentialStoreBuilder);
      return credentialStoreBuilder;
   }

   @Override
   public CredentialStoresConfiguration create() {
      Map<String, CredentialStoreConfiguration> map = credentialStores.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().create()));
      CredentialStoresConfiguration configuration = new CredentialStoresConfiguration(attributes.protect(), map, builder.properties());
      for(CredentialSupplier s : suppliers) {
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

   public Supplier<char[]> getCredential(String store, String alias) {
      CredentialSupplier credentialSupplier = new CredentialSupplier(store, alias);
      suppliers.add(credentialSupplier);
      return credentialSupplier;
   }

   private static class CredentialSupplier implements Supplier<char[]> {
      final String store;
      final String alias;
      CredentialStoresConfiguration configuration;

      CredentialSupplier(String store, String alias) {
         this.store = store;
         this.alias = alias;
      }

      @Override
      public char[] get() {
         return configuration.getCredential(store, alias);
      }
   }
}
