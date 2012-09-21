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

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1ConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<L1Configuration> {

   private static final Log log = LogFactory.getLog(L1ConfigurationBuilder.class);

   private boolean enabled = false;
   private int invalidationThreshold = 0;
   private long lifespan = TimeUnit.MINUTES.toMillis(10);
   private Boolean onRehash = null;
   private long cleanupTaskFrequency = TimeUnit.MINUTES.toMillis(10);

   L1ConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * <p>
    * Determines whether a multicast or a web of unicasts are used when performing L1 invalidations.
    * </p>
    *
    * <p>
    * By default multicast will be used.
    * </p>
    *
    * <p>
    * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to
    * 0, then multicast will be always be used.
    * </p>
    *
    * @param invalidationThreshold the threshold over which to use a multicast
    *
    */
   public L1ConfigurationBuilder invalidationThreshold(int invalidationThreshold) {
      this.invalidationThreshold = invalidationThreshold;
      return this;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long lifespan) {
      this.lifespan = lifespan;
      return this;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long lifespan, TimeUnit unit) {
      return lifespan(unit.toMillis(lifespan));
   }

   /**
    * How often the L1 requestors map is cleaned up of stale items
    */
   public L1ConfigurationBuilder cleanupTaskFrequency(long frequencyMillis) {
      this.cleanupTaskFrequency = frequencyMillis;
      return this;
   }

   /**
    * How often the L1 requestors map is cleaned up of stale items
    */
   public L1ConfigurationBuilder cleanupTaskFrequency(long frequencyMillis, TimeUnit unit) {
      return cleanupTaskFrequency(unit.toMillis(frequencyMillis));
   }

   /**
    * Entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    */
   public L1ConfigurationBuilder enableOnRehash() {
      this.onRehash = true;
      return this;
   }

   /**
    * Entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    */
   public L1ConfigurationBuilder onRehash(boolean enabled) {
      this.onRehash = enabled;
      return this;
   }

   /**
    * Entries removed due to a rehash will be removed altogether rather than bring moved to L1.
    */
   public L1ConfigurationBuilder disableOnRehash() {
      this.onRehash = false;
      return this;
   }

   public L1ConfigurationBuilder l1OnRehash(boolean l1OnRehash) {
      this.onRehash = l1OnRehash;
      return this;
   }

   public L1ConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public L1ConfigurationBuilder disable() {
      this.enabled = false;
      this.onRehash = null;
      return this;
   }

   public L1ConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      this.onRehash = enabled ? this.onRehash : null;
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (!clustering().cacheMode().isDistributed())
            throw new ConfigurationException("Enabling the L1 cache is only supported when using DISTRIBUTED as a cache mode.  Your cache mode is set to " + clustering().cacheMode().friendlyCacheModeString());

         if (lifespan < 1)
            throw new ConfigurationException("Using a L1 lifespan of 0 or a negative value is meaningless");

      }
      else {
         // If L1 is disabled, L1ForRehash should also be disabled
         if (onRehash != null && onRehash)
            throw new ConfigurationException("Can only move entries to L1 on rehash when L1 is enabled");
      }
   }

   @Override
   public L1Configuration create() {
      boolean finalOnRehash;
      if (onRehash != null) {
         finalOnRehash = onRehash;
      } else {
         finalOnRehash = false;
         if (enabled) {
            log.debug("L1 is enabled and L1OnRehash was not defined, enabling it");
            finalOnRehash = true;
         }
      }
      return new L1Configuration(enabled, invalidationThreshold, lifespan, finalOnRehash, cleanupTaskFrequency);
   }

   @Override
   public L1ConfigurationBuilder read(L1Configuration template) {
      enabled = template.enabled();
      invalidationThreshold = template.invalidationThreshold();
      lifespan = template.lifespan();
      onRehash = template.onRehash();
      cleanupTaskFrequency = template.cleanupTaskFrequency();
      return this;
   }

   @Override
   public String toString() {
      return "L1ConfigurationBuilder{" +
            "enabled=" + enabled +
            ", invalidationThreshold=" + invalidationThreshold +
            ", lifespan=" + lifespan +
            ", cleanupTaskFrequency=" + cleanupTaskFrequency +
            ", onRehash=" + onRehash +
            '}';
   }
}
