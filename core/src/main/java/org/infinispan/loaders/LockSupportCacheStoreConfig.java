/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders;


/**
 * Adds configuration support for {@link LockSupportCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class LockSupportCacheStoreConfig extends AbstractCacheStoreConfig {

   private static final long serialVersionUID = 842757200078048889L;
   
   public static final int DEFAULT_CONCURRENCY_LEVEL = 2048;
   public static final int DEFAULT_LOCK_ACQUISITION_TIMEOUT = 60000;

   private int lockConcurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
   private long lockAcquistionTimeout = DEFAULT_LOCK_ACQUISITION_TIMEOUT;

   /**
    * Returns number of threads expected to use this class concurrently.
    */
   public int getLockConcurrencyLevel() {
      return lockConcurrencyLevel;
   }

   /**
    * Sets number of threads expected to use this class concurrently.
    */
   public void setLockConcurrencyLevel(int lockConcurrencyLevel) {
      testImmutability("lockConcurrencyLevel");
      this.lockConcurrencyLevel = lockConcurrencyLevel;
   }

   public long getLockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }

   public void setLockAcquistionTimeout(long lockAcquistionTimeout) {
      testImmutability("lockAcquistionTimeout");
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   @Override
   public String toString() {
      return "LockSupportCacheStoreConfig{" +
            "lockConcurrencyLevel=" + lockConcurrencyLevel +
            ", lockAcquistionTimeout=" + lockAcquistionTimeout +
            "} " + super.toString();
   }
}
