package org.infinispan.persistence.factory;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Default implementation, which uses Local class loader. No external class loading is allowed.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class LocalClassLoaderCacheStoreFactory implements CacheStoreFactory {

   private static final Log log = LogFactory.getLog(LocalClassLoaderCacheStoreFactory.class);

   @Override
   public <T> T createInstance(StoreConfiguration cfg) {
      Class classBasedOnConfigurationAnnotation = ConfigurationForClassExtractor.getClassBasedOnConfigurationAnnotation(cfg, log);
      try {
         //getInstance is heavily used, so refactoring it might be risky. However we can safely catch
         //and ignore the exception. Returning null is perfectly legal here.
         Object instance = Util.getInstance(classBasedOnConfigurationAnnotation);
         if(instance != null) {
            return (T) instance;
         }
      } catch (CacheConfigurationException unableToInstantiate) {
         log.debugv("Could not instantiate class {0} using local classloader", classBasedOnConfigurationAnnotation.getName());
      }
      return null;
   }

   @Override
   public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
      return null;
   }

}
