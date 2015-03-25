package org.infinispan.persistence.factory;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.util.logging.Log;

public class ConfigurationForClassExtractor {

   public static Class getClassBasedOnConfigurationAnnotation(StoreConfiguration cfg, Log logger) {
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
         throw logger.loaderConfigurationDoesNotSpecifyLoaderClass(cfg.getClass().getName());
      }
      return classAnnotation;
   }

}
