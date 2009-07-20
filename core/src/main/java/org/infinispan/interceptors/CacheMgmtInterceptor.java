/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

import java.util.Map;
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

      if (data != null && data.size() > 0) {
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

   @ManagedAttribute(description = "number of cache attribute hits")
   public long getHits() {
      return hits.get();
   }

   @ManagedAttribute(description = "number of cache attribute misses")
   public long getMisses() {
      return misses.get();
   }

   @ManagedAttribute(description = "number of cache attribute put operations")
   public long getStores() {
      return stores.get();
   }

   @ManagedAttribute(description = "number of cache eviction operations")
   public long getEvictions() {
      return evictions.get();
   }

   @ManagedAttribute(description = "hit/(hit+miss) ratio for the cache")
   public double getHitRatio() {
      double total = hits.get() + misses.get();
      if (total == 0)
         return 0;
      return (hits.get() / total);
   }

   @ManagedAttribute(description = "read/writes ratio for the cache")
   public double getReadWriteRatio() {
      if (stores.get() == 0)
         return 0;
      return (((double) (hits.get() + misses.get()) / (double) stores.get()));
   }

   @ManagedAttribute(description = "average number of milliseconds for a read operation")
   public long getAverageReadTime() {
      long total = hits.get() + misses.get();
      if (total == 0)
         return 0;
      return (hitTimes.get() + missTimes.get()) / total;
   }

   @ManagedAttribute(description = "average number of milliseconds for a write operation")
   public long getAverageWriteTime() {
      if (stores.get() == 0)
         return 0;
      return (storeTimes.get()) / stores.get();
   }

   @ManagedAttribute(description = "number of entries in the cache")
   public int getNumberOfEntries() {
      return dataContainer.size();
   }

   @ManagedAttribute(description = "seconds since cache started")
   public long getElapsedTime() {
      return (System.currentTimeMillis() - start.get()) / 1000;
   }

   @ManagedAttribute(description = "number of seconds since the cache statistics were last reset")
   public long getTimeSinceReset() {
      return (System.currentTimeMillis() - reset.get()) / 1000;
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   public void resetStatistics() {
      hits.set(0);
      misses.set(0);
      stores.set(0);
      evictions.set(0);
      hitTimes.set(0);
      missTimes.set(0);
      storeTimes.set(0);
      reset.set(System.currentTimeMillis());
   }
}

