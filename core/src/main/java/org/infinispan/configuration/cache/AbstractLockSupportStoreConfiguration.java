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

import org.infinispan.util.TypedProperties;

/**
 * Lock supporting cache store configuration.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractLockSupportStoreConfiguration extends AbstractStoreConfiguration implements LockSupportStoreConfiguration {

   private final int lockConcurrencyLevel;
   private final long lockAcquistionTimeout;

   protected AbstractLockSupportStoreConfiguration(long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
         int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
         TypedProperties properties, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      this.lockConcurrencyLevel = lockConcurrencyLevel;
   }

   @Override
   public long lockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }

   @Override
   public int lockConcurrencyLevel() {
      return lockConcurrencyLevel;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      AbstractLockSupportStoreConfiguration that = (AbstractLockSupportStoreConfiguration) o;

      if (lockAcquistionTimeout != that.lockAcquistionTimeout) return false;
      if (lockConcurrencyLevel != that.lockConcurrencyLevel) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + lockConcurrencyLevel;
      result = 31 * result + (int) (lockAcquistionTimeout ^ (lockAcquistionTimeout >>> 32));
      return result;
   }

}
