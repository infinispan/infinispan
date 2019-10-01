package org.infinispan.counter.impl.manager;

import static org.infinispan.counter.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.Map;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A volatile implementation of {@link CounterConfigurationStorage}.
 * <p>
 * It throws an exception if it tries to store a {@link Storage#PERSISTENT} counter.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class VolatileCounterConfigurationStorage implements CounterConfigurationStorage {

   @Override
   public Map<String, CounterConfiguration> loadAll() {
      return Collections.emptyMap();
   }

   @Override
   public void store(String name, CounterConfiguration configuration) {

   }

   @Override
   public void remove(String name) {

   }

   @Override
   public void validatePersistence(CounterConfiguration configuration) {
      if (configuration.storage() == Storage.PERSISTENT) {
         throw CONTAINER.invalidPersistentStorageMode();
      }
   }

   @Override
   public void initialize(EmbeddedCacheManager cacheManager) {
      //no-op
   }
}
