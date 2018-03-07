package org.infinispan.counter.impl.manager;

import java.util.Collections;
import java.util.Map;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.logging.Log;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

/**
 * A volatile implementation of {@link CounterConfigurationStorage}.
 * <p>
 * It throws an exception if it tries to store a {@link Storage#PERSISTENT} counter.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class VolatileCounterConfigurationStorage implements CounterConfigurationStorage {

   private static final Log log = LogFactory.getLog(VolatileCounterConfigurationStorage.class, Log.class);

   @Override
   public Map<String, CounterConfiguration> loadAll() {
      return Collections.emptyMap();
   }

   @Override
   public void store(String name, CounterConfiguration configuration) {

   }

   @Override
   public void validatePersistence(CounterConfiguration configuration) {
      if (configuration.storage() == Storage.PERSISTENT) {
         throw log.invalidPersistentStorageMode();
      }
   }

   @Override
   public void initialize(EmbeddedCacheManager cacheManager) {
      //no-op
   }
}
