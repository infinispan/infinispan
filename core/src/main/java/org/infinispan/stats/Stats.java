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

/**
 * Stats.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Stats {

   /**
    * @return Number of seconds since cache started.
    */
   long getTimeSinceStart();

   /**
    * Returns the number of entries currently in this cache instance. When
    * the cache is configured with distribution, this method only returns the
    * number of entries in the local cache instance. In other words, it does
    * not attempt to communicate with other nodes to find out about the data
    * stored in other nodes in the cluster that is not available locally.
    *
    * @return Number of entries currently in the cache.
    */
   int getCurrentNumberOfEntries();

   /**
    * Number of entries stored in cache since the cache started running.
    */
   long getTotalNumberOfEntries();

   /**
    * @return Number of put operations on the cache.
    */
   long getStores();

   /**
    * @return Number of get operations.
    */
   long getRetrievals();

   /**
    * @return Number of cache get hits.
    */
   long getHits();

   /**
    * @return Number of cache get misses.
    */
   long getMisses();

   /**
    * @return Number of cache removal hits.
    */
   long getRemoveHits();

   /**
    * @return Number of cache removal misses.
    */
   long getRemoveMisses();

   /**
    * @return Number of cache eviction.
    */   
   long getEvictions();
}
