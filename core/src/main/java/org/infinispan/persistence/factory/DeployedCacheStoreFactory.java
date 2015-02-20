package org.infinispan.persistence.factory;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.persistence.factory.ConfigurationForClassExtractor.getClassBasedOnConfigurationAnnotation;

/**
 * Cache Store factory designed for deployed instances.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class DeployedCacheStoreFactory implements CacheStoreFactory {

   private static final Log log = LogFactory.getLog(DeployedCacheStoreFactory.class);

   private Map<String, Object> instances = Collections.synchronizedMap(new HashMap<String, Object>());

   @Override
   public Object createInstance(StoreConfiguration cfg) {
      String classNameBasedOnConfigurationAnnotation = getClassBasedOnConfigurationAnnotation(cfg, log).getName();
      return instances.get(classNameBasedOnConfigurationAnnotation);
   }

   /**
    * Adds deployed instance of a Cache Store.
    *
    * @param className Name of the deployed class. Use <code>myObject.getClass().getName();</code>
    * @param instance Instance.
    */
   public void addInstance(String className, Object instance) {
      instances.put(className, instance);
   }

   /**
    * Removed deployed instance of a Cache Store.
    *
    * @param className Name of the deployed class.
    */
   public void removeInstance(String className) {
      instances.remove(className);
   }

}
