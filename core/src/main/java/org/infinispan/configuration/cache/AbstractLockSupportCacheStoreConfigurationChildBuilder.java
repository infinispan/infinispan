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

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public abstract class AbstractLockSupportCacheStoreConfigurationChildBuilder<T>
      extends AbstractLoaderConfigurationChildBuilder<T> {

   // TODO Add defaults
   private int lockConcurrencyLevel;
   private long lockAcquistionTimeout;

   protected AbstractLockSupportCacheStoreConfigurationChildBuilder(LoaderConfigurationBuilder builder) {
      super(builder);
   }

   public AbstractLockSupportCacheStoreConfigurationChildBuilder lockConcurrencyLevel(int lockConcurrencyLevel) {
      this.lockConcurrencyLevel = lockConcurrencyLevel;
      return this;
   }

   public AbstractLockSupportCacheStoreConfigurationChildBuilder lockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      return this;
   }

}
