package org.infinispan.configuration.serializing;

import java.util.Map;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * ConfigurationHolder.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ConfigurationHolder {
   private final GlobalConfiguration globalConfiguration;
   private final Map<String, Configuration> configurations;

   public ConfigurationHolder(GlobalConfiguration globalConfiguration, Map<String, Configuration> configurations) {
      this.globalConfiguration = globalConfiguration;
      this.configurations = configurations;
   }

   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   public Map<String, Configuration> getConfigurations() {
      return configurations;
   }

}
