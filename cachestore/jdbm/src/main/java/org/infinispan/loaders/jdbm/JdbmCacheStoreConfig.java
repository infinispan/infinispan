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
package org.infinispan.loaders.jdbm;

import java.util.Comparator;

import org.infinispan.config.Dynamic;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.util.Util;

/**
 * Configures {@link JdbmCacheStore}.
 *
 * @author Elias Ross
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JdbmCacheStoreConfig extends LockSupportCacheStoreConfig {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -3686035269816837880L;
   /**
    * @configRef desc="A location on disk where the store can write internal files"
    */
   String location = "jdbm";
   /**
    * @configRef desc="Comparator class used to sort the keys by the cache loader.
    * This should only need to be set when using keys that do not have a natural ordering."
    */
   String comparatorClassName = NaturalComparator.class.getName();

   /**
    * @configRef desc="Whenever a new entry is stored, an expiry entry is created and added
    * to the a queue that is later consumed by the eviction thread. This parameter sets the size
    * of this queue."
    */
   @Dynamic
   int expiryQueueSize = 10000;

   public JdbmCacheStoreConfig() {
      setCacheLoaderClassName(JdbmCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }

   public String getComparatorClassName() {
      return comparatorClassName;
   }

   public void setComparatorClassName(String comparatorClassName) {
      testImmutability("comparatorClassName");
      this.comparatorClassName = comparatorClassName;
   }

   public int getExpiryQueueSize() {
      return expiryQueueSize;
   }

   public void setExpiryQueueSize(int expiryQueueSize) {
      testImmutability("expiryQueueSize");
      this.expiryQueueSize = expiryQueueSize;
   }

   /**
    * Returns a new comparator instance based on {@link #setComparatorClassName(String)}.
    */
   public Comparator createComparator() {
      return (Comparator) Util.getInstance(comparatorClassName, getClassLoader());
   }

}
