/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

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
