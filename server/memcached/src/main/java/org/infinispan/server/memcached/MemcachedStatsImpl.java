/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached;

import java.util.List;

import org.infinispan.stats.Stats;
import org.infinispan.server.memcached.InterceptorChain;

/**
 * MemcachedStats.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MemcachedStatsImpl implements MemcachedStats, Stats {
   final Stats cacheStats;
   final long incrMisses;
   final long incrHits;
   final long decrMisses;
   final long decrHits;
   final long casMisses;
   final long casHits;
   final long casBadval;

   MemcachedStatsImpl(Stats cacheStats, InterceptorChain chain) {
      this.cacheStats = cacheStats;
      List<CommandInterceptor> interceptors = chain.getInterceptorsWhichExtend(StatsInterceptor.class);
      if (!interceptors.isEmpty()) {
         StatsInterceptor statsInt = (StatsInterceptor) interceptors.get(0);
         incrMisses = statsInt.getIncrMisses();
         incrHits = statsInt.getIncrHits();
         decrMisses = statsInt.getDecrMisses();
         decrHits = statsInt.getDecrHits();
         casMisses = statsInt.getCasMisses();
         casHits = statsInt.getCasHits();
         casBadval = statsInt.getCasBadval();
      } else {
         incrMisses = -1;
         incrHits = -1;
         decrMisses = -1;
         decrHits = -1;
         casMisses = -1;
         casHits = -1;
         casBadval = -1;
      }
   }

   public long getCasBadval() {
      return casBadval;
   }

   public long getCasHits() {
      return casHits;
   }

   public long getCasMisses() {
      return casMisses;
   }

   public long getDecrHits() {
      return decrHits;
   }

   public long getDecrMisses() {
      return decrMisses;
   }

   public long getIncrHits() {
      return incrHits;
   }

   public long getIncrMisses() {
      return incrMisses;
   }

   public int getCurrentNumberOfEntries() {
      return cacheStats.getCurrentNumberOfEntries();
   }

   public long getEvictions() {
      return cacheStats.getEvictions();
   }

   public long getHits() {
      return cacheStats.getHits();
   }

   public long getMisses() {
      return cacheStats.getMisses();
   }

   public long getRemoveHits() {
      return cacheStats.getRemoveHits();
   }

   public long getRemoveMisses() {
      return cacheStats.getRemoveMisses();
   }

   public long getRetrievals() {
      return cacheStats.getRetrievals();
   }

   public long getStores() {
      return cacheStats.getStores();
   }

   public long getTimeSinceStart() {
      return cacheStats.getTimeSinceStart();
   }

   public long getTotalNumberOfEntries() {
      return cacheStats.getTotalNumberOfEntries();
   }
   
}
