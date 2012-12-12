/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.eviction;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Concrete implementation of activation logic manager.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@MBean(objectName = "Activation",
      description = "Component that handles activating entries that have been passivated to a CacheStore by loading them into memory.")
public class ActivationManagerImpl implements ActivationManager {

   private static final Log log = LogFactory.getLog(ActivationManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private final AtomicLong activations = new AtomicLong(0);
   private CacheLoaderManager clm;
   private CacheStore store;
   private Configuration cfg;
   private boolean enabled;

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   private boolean statisticsEnabled = false;

   @Inject
   public void inject(CacheLoaderManager clm, Configuration cfg) {
      this.clm = clm;
      this.cfg = cfg;
   }

   @Start(priority = 10) // Just before the passivation manager
   public void start() {
      enabled = clm.isUsingPassivation() && !clm.isShared();
      if (enabled) {
         store = clm.getCacheStore();
         if (store == null)
            throw new ConfigurationException(
                  "Passivation can only be used with a CacheLoader that implements CacheStore!");

         statisticsEnabled = cfg.jmxStatistics().enabled();
      }
   }

   @Override
   public void activate(Object key) {
      if (enabled) {
         try {
            if (store.remove(key) && statisticsEnabled) {
               activations.incrementAndGet();
            }
         } catch (CacheLoaderException e) {
            log.unableToRemoveEntryAfterActivation(key, e);
         }
      } else {
         if (trace)
            log.trace("Don't remove entry from shared cache store after activation.");
      }
   }

   @Override
   public long getActivationCount() {
      return activations.get();
   }

   @ManagedAttribute(description = "Number of activation events")
   @Metric(displayName = "Number of cache entries activated",
         measurementType = MeasurementType.TRENDSUP)
   @SuppressWarnings("unused")
   public String getActivations() {
      if (!statisticsEnabled)
         return "N/A";

      return String.valueOf(getActivationCount());
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      activations.set(0);
   }
}

