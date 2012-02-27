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
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfiguration {

   private final long lifespan;
   private final long maxIdle;
   private final boolean reaperEnabled;
   private final long wakeUpInterval;

   ExpirationConfiguration(long lifespan, long maxIdle, boolean reaperEnabled, long wakeUpInterval) {
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.reaperEnabled = reaperEnabled;
      this.wakeUpInterval = wakeUpInterval;
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long lifespan() {
      return lifespan;
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long maxIdle() {
      return maxIdle;
   }

   /**
    * Determines whether the background reaper thread is enabled to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public boolean reaperEnabled() {
      return reaperEnabled;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public long wakeUpInterval() {
      return wakeUpInterval;
   }

   @Override
   public String toString() {
      return "ExpirationConfiguration{" +
            "lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", reaperEnabled=" + reaperEnabled +
            ", wakeUpInterval=" + wakeUpInterval +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ExpirationConfiguration that = (ExpirationConfiguration) o;

      if (lifespan != that.lifespan) return false;
      if (maxIdle != that.maxIdle) return false;
      if (reaperEnabled != that.reaperEnabled) return false;
      if (wakeUpInterval != that.wakeUpInterval) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (lifespan ^ (lifespan >>> 32));
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result + (reaperEnabled ? 1 : 0);
      result = 31 * result + (int) (wakeUpInterval ^ (wakeUpInterval >>> 32));
      return result;
   }

}
