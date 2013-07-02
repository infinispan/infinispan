package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 * LockSupportCacheStoreConfigurationBuilder is an interface which should be implemented by all cache store builders which support locking
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface LockSupportStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {

   S lockAcquistionTimeout(long lockAcquistionTimeout);

   S lockAcquistionTimeout(
         long lockAcquistionTimeout, TimeUnit unit);

   S lockConcurrencyLevel(int lockConcurrencyLevel);

}