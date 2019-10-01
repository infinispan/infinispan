package org.infinispan.persistence.factory;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;

public class ConfigurationForClassExtractor {

   public static Class getClassBasedOnConfigurationAnnotation(StoreConfiguration cfg) {
      ConfigurationFor annotation = cfg.getClass().getAnnotation(ConfigurationFor.class);
      Class classAnnotation = null;
      if (annotation == null) {
         if (cfg instanceof CustomStoreConfiguration) {
            classAnnotation = ((CustomStoreConfiguration)cfg).customStoreClass();
         }
      } else {
         classAnnotation = annotation.value();
      }
      if (classAnnotation == null) {
         throw CONFIG.loaderConfigurationDoesNotSpecifyLoaderClass(cfg.getClass().getName());
      }
      return classAnnotation;
   }

}
