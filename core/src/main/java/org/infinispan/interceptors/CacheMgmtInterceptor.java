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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
   private final AtomicLong hitTimes = new AtomicLong(0);
   private final AtomicLong missTimes = new AtomicLong(0);
   private final AtomicLong storeTimes = new AtomicLong(0);
   private final AtomicLong hits = new AtomicLong(0);
   private final AtomicLong misses = new AtomicLong(0);
   private final AtomicLong stores = new AtomicLong(0);
   private final AtomicLong evictions = new AtomicLong(0);
   private final AtomicLong startNanoseconds = new AtomicLong(System.nanoTime());
   private final AtomicLong resetNanoseconds = new AtomicLong(startNanoseconds.get());
   private final AtomicLong removeHits = new AtomicLong(0);
   private final AtomicLong removeMisses = new AtomicLong(0);

   private DataContainer dataContainer;

   private static final Log log = LogFactory.getLog(CacheMgmtInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

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
      long t1 = System.nanoTime();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.nanoTime();
      long intervalMilliseconds = nanosecondsIntervalToMilliseconds(t1, t2);
      if (retval == null) {
         missTimes.getAndAdd(intervalMilliseconds);
         misses.incrementAndGet();
      } else {
         hitTimes.getAndAdd(intervalMilliseconds);
         hits.incrementAndGet();
      }
      return retval;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Map<Object, Object> data = command.getMap();
      long t1 = System.nanoTime();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.nanoTime();
      long intervalMilliseconds = nanosecondsIntervalToMilliseconds(t1, t2);
      if (data != null && !data.isEmpty()) {
         storeTimes.getAndAdd(intervalMilliseconds);
         stores.getAndAdd(data.size());
      }
      return retval;
   }

   @Override
   //Map.put(key,value) :: oldValue
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      long t1 = System.nanoTime();
      Object retval = invokeNextInterceptor(ctx, command);
      long t2 = System.nanoTime();
      long intervalMilliseconds = nanosecondsIntervalToMilliseconds(t1, t2);
      storeTimes.getAndAdd(intervalMilliseconds);
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
      long hitsL = hits.get();
      double total = hitsL + misses.get();
      // The reason for <= is that equality checks
      // should be avoided for floating point numbers.
      if (total <= 0)
         return 0;
      return (hitsL / total);
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
      return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanoseconds.get());
   }

   @ManagedAttribute(description = "Number of seconds since the cache statistics were last reset")
   @Metric(displayName = "Seconds since cache statistics were reset", units = Units.SECONDS, displayType = DisplayType.SUMMARY)
   public long getTimeSinceReset() {
      return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - resetNanoseconds.get());
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
      resetNanoseconds.set(System.nanoTime());
   }

   /**
    * @param nanoStart
    * @param nanoEnd
    * @return the interval rounded in milliseconds
    */
   private long nanosecondsIntervalToMilliseconds(final long nanoStart, final long nanoEnd) {
      return TimeUnit.MILLISECONDS.convert(nanoEnd - nanoStart, TimeUnit.NANOSECONDS);
   }
}

