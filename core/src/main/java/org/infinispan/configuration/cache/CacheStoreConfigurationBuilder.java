package org.infinispan.configuration.cache;

/**
 * StoreConfigurationBuilder is the interface which should be implemented by all cache store builders
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface CacheStoreConfigurationBuilder<T extends CacheStoreConfiguration, S extends CacheStoreConfigurationBuilder<T, S>> extends CacheLoaderConfigurationBuilder<T, S>, StoreConfigurationChildBuilder<S> {


}