package org.infinispan.multimap.api.embedded;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.multimap.configuration.EmbeddedMultimapConfiguration;

@Experimental
public interface MultimapCacheManager<K, V> {

   /**
    * Defines a named multimap cache's configuration by using the provided configuration
    * If this cache was already configured, either declaratively or programmatically, this method will throw a
    * {@link org.infinispan.commons.CacheConfigurationException}.
    * <p>
    * A {@link org.infinispan.multimap.configuration.MultimapCacheManagerConfiguration} or a
    * {@link EmbeddedMultimapConfiguration} can be provided within the configuration. The former takes precedence.
    * <p>
    * If no {@link org.infinispan.multimap.configuration.MultimapCacheManagerConfiguration} or
    * {@link EmbeddedMultimapConfiguration} is provided.
    * <p>
    * Currently, the MultimapCache with the given name "foo" can be also accessed as a regular cache named "foo",
    * there is no assertion on the use, possibly leading to type errors.
    *
    * @param name          name of multimap cache whose configuration is being defined
    * @param configuration configuration overrides to use
    * @return a cloned configuration instance
    * @deprecated Since 15, use {@link #defineConfiguration(Configuration, EmbeddedMultimapConfiguration)} instead.
    */
   Configuration defineConfiguration(String name, Configuration configuration);

   /**
    * Defines a cache and multimap configuration.
    * <p>
    * This method receives both a {@link Configuration} for the cache and a {@link EmbeddedMultimapConfiguration} for
    * the multimap. If the cache was already configured, either declaratively or programmatically, this method will
    * throw a {@link org.infinispan.commons.CacheConfigurationException}.
    * <p>
    * A {@link EmbeddedMultimapConfiguration} defined declaratively is not overridable programmatically.
    *
    * @param cacheConfiguration: configuration for the underlying cache.
    * @param multimapConfiguration: multimap configuration.
    * @return A completion true if the {@link EmbeddedMultimapConfiguration} was applied, false otherwise.
    * @throws org.infinispan.commons.CacheConfigurationException if the cache was already configured.
    * @see #defineConfiguration(String, Configuration).
    */
   CompletionStage<Boolean> defineConfiguration(Configuration cacheConfiguration, EmbeddedMultimapConfiguration multimapConfiguration);

   /**
    * Defines a multimap configuration.
    * <p>
    * A {@link EmbeddedMultimapConfiguration} defined declaratively is not overridable programmatically.
    *
    * @param configuration: multimap configuration.
    * @return A completion true if the {@link EmbeddedMultimapConfiguration} was applied, false otherwise.
    * @see #defineConfiguration(String, Configuration)
    * @see #defineConfiguration(Configuration, EmbeddedMultimapConfiguration)
    */
   CompletionStage<Boolean> defineConfiguration(EmbeddedMultimapConfiguration configuration);

   /**
    * Retrieves a named multimap cache from the system.
    *
    * @param name, name of multimap cache to retrieve
    * @return null if no configuration exists as per rules set above, otherwise returns a multimap cache instance
    * identified by cacheName and doesn't support duplicates
    */
   default MultimapCache<K, V> get(String name) {
      return get(name, false);
   }

   /**
    * Retrieves a named multimap cache from the system.
    *
    * @param name, name of multimap cache to retrieve
    * @param supportsDuplicates, boolean check to see whether duplicates are supported or not
    * @return null if no configuration exists as per rules set above, otherwise returns a multimap cache instance
    * identified by cacheName
    */
   MultimapCache<K, V> get(String name, boolean supportsDuplicates);
}
