package org.infinispan.configuration.cache;

/**
 * LockSupportCacheStoreConfigurationBuilder is an interface which should be implemented by all cache store builders which support locking
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface LockSupportStoreConfigurationBuilder<T extends LockSupportStoreConfiguration, S extends LockSupportStoreConfigurationBuilder<T, S>> extends CacheStoreConfigurationBuilder<T, S>, LockSupportStoreConfigurationChildBuilder<S> {

}