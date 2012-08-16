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
 * AbstractLockSupportCacheStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractLockSupportCacheStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractLockSupportCacheStoreConfigurationBuilder<T, S>> extends
      AbstractStoreConfigurationBuilder<T, S> implements LockSupportCacheStoreConfigurationBuilder<T, S> {

   protected long lockAcquistionTimeout;
   protected int lockConcurrencyLevel;

   public AbstractLockSupportCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
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
