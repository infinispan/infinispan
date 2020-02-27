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

   private static final String INFINISPAN_BACKEND_NAME = "infinispan_backend";
   private static final String BACKEND_PROPERTIES_PREFIX = EngineSettings.BACKENDS + "." + INFINISPAN_BACKEND_NAME;

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
      properties.entrySet().stream()
            .forEach(property -> setProperty(property.getKey(), property.getValue()) );
   }

   public ConfigurationPropertySource createPropertySource(ConfigurationPropertyChecker propertyChecker) {
      ConfigurationPropertySource basePropertySource =
            propertyChecker.wrap(ConfigurationPropertySource.fromMap(backendProperties))
                  .withPrefix(BACKEND_PROPERTIES_PREFIX);
      ConfigurationPropertySource propertySource =
            basePropertySource.withOverride(ConfigurationPropertySource.fromMap(engineProperties));
      defaultProperties();
      return propertySource;
   }

   private void defaultProperties() {
      engineProperties.put("hibernate.search.default_backend", INFINISPAN_BACKEND_NAME);
      backendProperties.put("type", "lucene");
      backendProperties.put("analysis.configurer", new DefaultAnalysisConfigurer());
      backendProperties.put("thread_pool.size", "1");
      backendProperties.put("index_defaults.indexing.queue_count", "1");
   }
}
