package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.configuration.ServerConfigurationBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class ProvidersConfigurationBuilder implements Builder<ProvidersConfiguration> {

   private final AttributeSet attributes;
   private final List<ProviderConfigurationBuilder> providers = new ArrayList<>(2);
   private final ServerConfigurationBuilder builder;

   public ProvidersConfigurationBuilder(ServerConfigurationBuilder builder) {
      this.builder = builder;
      this.attributes = ProvidersConfiguration.attributeDefinitionSet();
   }

   public ProviderConfigurationBuilder addProvider() {
      ProviderConfigurationBuilder providerConfigurationBuilder = new ProviderConfigurationBuilder();
      providers.add(providerConfigurationBuilder);
      return providerConfigurationBuilder;
   }

   @Override
   public ProvidersConfiguration create() {
      List<ProviderConfiguration> list = providers.stream().map(ProviderConfigurationBuilder::create).collect(Collectors.toList());
      return new ProvidersConfiguration(attributes.protect(), list, builder.properties());
   }

   @Override
   public Builder<?> read(ProvidersConfiguration template) {
      providers.clear();
      template.providers().forEach(p -> addProvider().read(p));
      return this;
   }
}
