package org.infinispan.configuration.parsing;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class ConfigurationBuilderHolder {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final ConfigurationBuilder defaultConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   
   public ConfigurationBuilderHolder() {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.defaultConfigurationBuilder = new ConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<String, ConfigurationBuilder>();
   }
   
   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }
   
   public ConfigurationBuilder newConfigurationBuilder(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //no need to validate default config again 
      //https://issues.jboss.org/browse/ISPN-1938
      builder.read(getDefaultConfigurationBuilder().build(false));
      namedConfigurationBuilders.put(name, builder);
      return builder;
   }
   
   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      return defaultConfigurationBuilder;
   }
   
   public Map<String, ConfigurationBuilder> getNamedConfigurationBuilders() {
      return namedConfigurationBuilders;
   }
   
}
