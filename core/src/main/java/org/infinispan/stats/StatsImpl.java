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
package org.infinispan.stats;

import java.util.List;

import net.jcip.annotations.Immutable;

import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * StatsImpl.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class StatsImpl implements Stats {
   final long timeSinceStart;
   final int currentNumberOfEntries;
   final long totalNumberOfEntries;
   final long retrievals;
   final long stores;
   final long hits;
   final long misses;
   final long removeHits;
   final long removeMisses;
   final long evictions;
   
   public StatsImpl(InterceptorChain chain) {
      List<CommandInterceptor> interceptors = chain.getInterceptorsWhichExtend(CacheMgmtInterceptor.class);
      if (!interceptors.isEmpty()) {
         CacheMgmtInterceptor mgmtInterceptor = (CacheMgmtInterceptor) interceptors.get(0);
         timeSinceStart = mgmtInterceptor.getElapsedTime();
         currentNumberOfEntries = mgmtInterceptor.getNumberOfEntries();
         totalNumberOfEntries = mgmtInterceptor.getStores();
         retrievals = mgmtInterceptor.getHits() + mgmtInterceptor.getMisses();
         stores = mgmtInterceptor.getStores();
         hits = mgmtInterceptor.getHits();
         misses = mgmtInterceptor.getMisses();
         removeHits = mgmtInterceptor.getRemoveHits();
         removeMisses = mgmtInterceptor.getRemoveMisses();
         evictions = mgmtInterceptor.getEvictions();
      } else {
         timeSinceStart = -1;
         currentNumberOfEntries = -1;
         totalNumberOfEntries = -1;
         retrievals = -1;
         stores = -1;
         hits = -1;
         misses = -1;
         removeHits = -1;
         removeMisses = -1;
         evictions = -1;
      }
   }

   public long getTimeSinceStart() {
      return timeSinceStart;
   }

   public int getCurrentNumberOfEntries() {
      return currentNumberOfEntries;
   }

   public long getTotalNumberOfEntries() {
      return totalNumberOfEntries;
   }

   public long getRetrievals() {
      return retrievals;
   }

   public long getStores() {
      return stores;
   }

   public long getHits() {
      return hits;
   }

   public long getMisses() {
      return misses;
   }

   public long getRemoveHits() {
      return removeHits;
   }

   public long getRemoveMisses() {
      return removeMisses;
   }

   public long getEvictions() {
      return evictions;
   }

}
