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
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfigurationBuilder extends AbstractConfigurationChildBuilder<ExpirationConfiguration> {

   private long lifespan = -1L;
   private long maxIdle = -1L;
   private boolean reaperEnabled = true;
   private long wakeUpInterval = TimeUnit.MINUTES.toMillis(1);
   
   ExpirationConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder lifespan(long l) {
      this.lifespan = l;
      return this;
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder maxIdle(long l) {
      this.maxIdle = l;
      return this;
   }

   /**
    * Enable the background reaper to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder enableReaper() {
      this.reaperEnabled = true;
      return this;
   }
   
   /**
    * Enable the background reaper to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder reaperEnabled(boolean enabled) {
      this.reaperEnabled = enabled;
      return this;
   }
   
   /**
    * Disable the background reaper to test entries for expiration. to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder disableReaper() {
      this.reaperEnabled = false;
      return this;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public ExpirationConfigurationBuilder wakeUpInterval(long l) {
      this.wakeUpInterval = l;
      return this;
   }

   @Override
   void validate() {
   }

   @Override
   ExpirationConfiguration create() {
      return new ExpirationConfiguration(lifespan, maxIdle, reaperEnabled, wakeUpInterval);
   }
   
   @Override
   public ExpirationConfigurationBuilder read(ExpirationConfiguration template) {
      this.lifespan = template.lifespan();
      this.maxIdle = template.maxIdle();
      this.reaperEnabled = template.reaperEnabled();
      this.wakeUpInterval = template.wakeUpInterval();
      
      return this;
   }

   @Override
   public String toString() {
      return "ExpirationConfigurationBuilder{" +
            "lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", reaperEnabled=" + reaperEnabled +
            ", wakeUpInterval=" + wakeUpInterval +
            '}';
   }

}
