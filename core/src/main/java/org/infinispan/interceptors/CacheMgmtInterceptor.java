/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures cache management statistics
 *
 * @author Jerry Gauthier
 * @since 4.0
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {
   private AtomicLong hitTimes = new AtomicLong(0);
   private AtomicLong missTimes = new AtomicLong(0);
   private AtomicLong storeTimes = new AtomicLong(0);
   private AtomicLong hits = new AtomicLong(0);
   private AtomicLong misses = new AtomicLong(0);
   private AtomicLong stores = new AtomicLong(0);
   private AtomicLong evictions = new AtomicLong(0);
   private AtomicLong start = new AtomicLong(System.currentTimeMillis());
   private AtomicLong reset = new AtomicLong(start.get());
   private AtomicLong removeHits = new AtomicLong(0);
   private AtomicLong removeMisses = new AtomicLong(0);

   private DataContainer dataContainer;

   @Inject
   public void setDependencies(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      evictions.incrementAndGet();
      return returnValue;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      long t1 = System.currentTimeMillis();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.currentTimeMillis();
      if (retval == null) {
         missTimes.getAndAdd(t2 - t1);
         misses.incrementAndGet();
      } else {
         hitTimes.getAndAdd(t2 - t1);
         hits.incrementAndGet();
      }
      return retval;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Map data = command.getMap();
      long t1 = System.currentTimeMillis();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.currentTimeMillis();

      if (data != null && !data.isEmpty()) {
         storeTimes.getAndAdd(t2 - t1);
         stores.getAndAdd(data.size());
      }
      return retval;
   }

   @Override
   //Map.put(key,value) :: oldValue
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      long t1 = System.currentTimeMillis();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.currentTimeMillis();
      storeTimes.getAndAdd(t2 - t1);
      stores.incrementAndGet();
      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (retval == null) {
         removeMisses.incrementAndGet();
      } else {
         removeHits.incrementAndGet();
      }
      return retval;
   }

   @ManagedAttribute(description = "Number of cache attribute hits")
   @Metric(displayName = "Number of cache hits", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getHits() {
      return hits.get();
   }

   @ManagedAttribute(description = "Number of cache attribute misses")
   @Metric(displayName = "Number of cache misses", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getMisses() {
      return misses.get();
   }

   @ManagedAttribute(description = "Number of cache removal hits")
   @Metric(displayName = "Number of cache removal hits", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getRemoveHits() {
      return removeHits.get();
   }

   @ManagedAttribute(description = "Number of cache removals where keys were not found")
   @Metric(displayName = "Number of cache removal misses", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getRemoveMisses() {
      return removeMisses.get();
   }

   @ManagedAttribute(description = "number of cache attribute put operations")
   @Metric(displayName = "Number of cache puts" , measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getStores() {
      return stores.get();
   }

   @ManagedAttribute(description = "Number of cache eviction operations")
   @Metric(displayName = "Number of cache evictions", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getEvictions() {
      return evictions.get();
   }

   @ManagedAttribute(description = "Percentage hit/(hit+miss) ratio for the cache")
   @Metric(displayName = "Hit ratio", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getHitRatio() {
      double total = hits.get() + misses.get();
      if (total == 0)
         return 0;
      return (hits.get() / total);
   }

   @ManagedAttribute(description = "read/writes ratio for the cache")
   @Metric(displayName = "Read/write ratio", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getReadWriteRatio() {
      if (stores.get() == 0)
         return 0;
      return (((double) (hits.get() + misses.get()) / (double) stores.get()));
   }

   @ManagedAttribute(description = "Average number of milliseconds for a read operation on the cache")
   @Metric(displayName = "Average read time", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public long getAverageReadTime() {
      long total = hits.get() + misses.get();
      if (total == 0)
         return 0;
      return (hitTimes.get() + missTimes.get()) / total;
   }

   @ManagedAttribute(description = "Average number of milliseconds for a write operation in the cache")
   @Metric(displayName = "Average write time", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public long getAverageWriteTime() {
      if (stores.get() == 0)
         return 0;
      return (storeTimes.get()) / stores.get();
   }

   @ManagedAttribute(description = "Number of entries currently in the cache")
   @Metric(displayName = "Number of current cache entries", displayType = DisplayType.SUMMARY)
   public int getNumberOfEntries() {
      return dataContainer.size();
   }

   @ManagedAttribute(description = "Number of seconds since cache started")
   @Metric(displayName = "Seconds since cache started", units = Units.SECONDS, measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getElapsedTime() {
      return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start.get());
   }

   @ManagedAttribute(description = "Number of seconds since the cache statistics were last reset")
   @Metric(displayName = "Seconds since cache statistics were reset", units = Units.SECONDS, displayType = DisplayType.SUMMARY)
   public long getTimeSinceReset() {
      return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - reset.get());
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      hits.set(0);
      misses.set(0);
      stores.set(0);
      evictions.set(0);
      hitTimes.set(0);
      missTimes.set(0);
      storeTimes.set(0);
      removeHits.set(0);
      removeMisses.set(0);
      reset.set(System.currentTimeMillis());
   }
}

