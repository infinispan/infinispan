package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 *
 * AbstractLockSupportStoreConfigurationChildBuilder delegates {@link LockSupportStoreConfigurationChildBuilder} methods
 * to a specified {@link LockSupportStoreConfigurationBuilder}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractLockSupportStoreConfigurationChildBuilder<S>
      extends AbstractStoreConfigurationChildBuilder<S> implements LockSupportStoreConfigurationChildBuilder<S> {

   private final LockSupportStoreConfigurationBuilder<? extends AbstractLockSupportStoreConfiguration, ? extends LockSupportStoreConfigurationBuilder<?, ?>> builder;

   public AbstractLockSupportStoreConfigurationChildBuilder(
         AbstractLockSupportStoreConfigurationBuilder<? extends AbstractLockSupportStoreConfiguration, ? extends LockSupportStoreConfigurationBuilder<?, ?>> builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public S lockAcquistionTimeout(long lockAcquistionTimeout) {
      return (S) builder.lockAcquistionTimeout(lockAcquistionTimeout);
   }

   @Override
   public S lockAcquistionTimeout(long lockAcquistionTimeout, TimeUnit unit) {
      return (S) builder.lockAcquistionTimeout(lockAcquistionTimeout, unit);
   }

   @Override
   public S lockConcurrencyLevel(int lockConcurrencyLevel) {
      return (S) builder.lockConcurrencyLevel(lockConcurrencyLevel);
   }
}
