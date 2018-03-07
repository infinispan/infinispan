package org.infinispan.counter.impl.manager;

import java.util.Map;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A local storage to persist counter's {@link CounterConfiguration}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface CounterConfigurationStorage {

   /**
    * Invoked when starts, it returns all the persisted counter's.
    *
    * @return all the persisted counter's name and configurations.
    */
   Map<String, CounterConfiguration> loadAll();

   /**
    * Persists the counter's configuration.
    *
    * @param name          the counter's name.
    * @param configuration the counter's {@link CounterConfiguration}.
    */
   void store(String name, CounterConfiguration configuration);

   /**
    * Validates if the {@link CounterConfiguration} has a valid {@link org.infinispan.counter.api.Storage}.
    * <p>
    * It throws an exception if the implementation doesn't support one or more {@link org.infinispan.counter.api.Storage} modes.
    *
    * @param configuration the {@link CounterConfiguration} to check.
    */
   void validatePersistence(CounterConfiguration configuration);

   /**
    * Initializes this instance with the {@link EmbeddedCacheManager}.
    *
    * @param cacheManager the cache manager.
    */
   void initialize(EmbeddedCacheManager cacheManager);
}
