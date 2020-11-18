package org.infinispan.search.mapper.mapping.impl;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.engine.cfg.EngineSettings;
import org.hibernate.search.engine.cfg.spi.ConfigurationPropertyChecker;
import org.hibernate.search.engine.cfg.spi.ConfigurationPropertySource;

/**
 * @author Fabio Massimo Ercoli
 */
public class IndexProperties {

   private final Map<String, Object> engineProperties = new HashMap<>();
   private final Map<String, Object> backendProperties = new HashMap<>();

   public void setProperty(String key, Object value) {
      if (key.startsWith(EngineSettings.PREFIX)) {
         // engine properties are passed to Search as they are,
         // so they start with "hibernate.search."
         engineProperties.put(key, value);
      } else {
         // backend properties are passed with BACKEND_PROPERTIES_PREFIX,
         // so that the user doesn't need to set it for them
         backendProperties.put(key, value);
      }
   }

   public void setProperties(Map<String, Object> properties) {
      properties.forEach(this::setProperty);
   }

   public ConfigurationPropertySource createPropertySource(ConfigurationPropertyChecker propertyChecker) {
      ConfigurationPropertySource basePropertySource =
            propertyChecker.wrap(ConfigurationPropertySource.fromMap(backendProperties))
                  .withPrefix(EngineSettings.BACKEND);
      ConfigurationPropertySource propertySource =
            basePropertySource.withOverride(ConfigurationPropertySource.fromMap(engineProperties));
      defaultProperties();
      return propertySource;
   }

   private void defaultProperties() {
      backendProperties.put("type", "lucene");
      backendProperties.put("analysis.configurer", new DefaultAnalysisConfigurer());
   }
}
