package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 * AbstractLockSupportCacheStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractLockSupportStoreConfigurationBuilder<T extends LockSupportStoreConfiguration, S extends AbstractLockSupportStoreConfigurationBuilder<T, S>> extends
      AbstractStoreConfigurationBuilder<T, S> implements LockSupportStoreConfigurationBuilder<T, S> {

   protected long lockAcquistionTimeout = 60000;
   protected int lockConcurrencyLevel = 2048;

   public AbstractLockSupportStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public S lockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      return self();
   }

   @Override
   public S lockAcquistionTimeout(long lockAcquistionTimeout, TimeUnit unit) {
      return lockAcquistionTimeout(unit.toMillis(lockAcquistionTimeout));
   }

   @Override
   public S lockConcurrencyLevel(int lockConcurrencyLevel) {
      this.lockConcurrencyLevel = lockConcurrencyLevel;
      return self();
   }
}
