package org.infinispan.configuration.parsing;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class ConfigurationBuilderHolder {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final ConfigurationBuilder defaultConfigurationBuilder;
   private final Set<ConfigurationBuilder> namedConfigurationBuilders;
   
   public ConfigurationBuilderHolder() {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.defaultConfigurationBuilder = new ConfigurationBuilder();
      this.namedConfigurationBuilders = new HashSet<ConfigurationBuilder>();
   }
   
   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }
   
   public ConfigurationBuilder newConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(getDefaultConfigurationBuilder().build());
      namedConfigurationBuilders.add(builder);
      return builder;
   }
   
   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      return defaultConfigurationBuilder;
   }
   
   public Set<ConfigurationBuilder> getConfigurationBuilders() {
      return namedConfigurationBuilders;
   }
   
}
