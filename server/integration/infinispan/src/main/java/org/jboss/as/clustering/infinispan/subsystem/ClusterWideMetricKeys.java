/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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

package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.stats.ClusterCacheStats;

/**
 *
 * Keys for cluster wide stats.
 *
 * @author Tristan Tarrant
 * @author Vladimir Blagojevic
 *
 * @see ClusterCacheStats
 *
 */

public class ClusterWideMetricKeys {

   // LockManager
   public static final String NUMBER_OF_LOCKS_AVAILABLE = "clusterwide-number-of-locks-available";
   public static final String NUMBER_OF_LOCKS_HELD = "clusterwide-number-of-locks-held";

   // cache management interceptor
   public static final String AVERAGE_READ_TIME = "clusterwide-average-read-time";
   public static final String AVERAGE_WRITE_TIME = "clusterwide-average-write-time";
   public static final String AVERAGE_REMOVE_TIME = "clusterwide-average-remove-time";
   public static final String TIME_SINCE_START = "clusterwide-time-since-start";
   public static final String EVICTIONS = "clusterwide-evictions";
   public static final String HIT_RATIO = "clusterwide-hit-ratio";
   public static final String HITS = "clusterwide-hits";
   public static final String MISSES = "clusterwide-misses";
   public static final String NUMBER_OF_ENTRIES = "clusterwide-number-of-entries";
   public static final String READ_WRITE_RATIO = "clusterwide-read-write-ratio";
   public static final String REMOVE_HITS = "clusterwide-remove-hits";
   public static final String REMOVE_MISSES = "clusterwide-remove-misses";
   public static final String STORES = "clusterwide-stores";
   public static final String TIME_SINCE_RESET = "clusterwide-time-since-reset";

   // invalidation interceptor
   public static final String INVALIDATIONS = "clusterwide-invalidations";
   // passivation interceptor
   public static final String PASSIVATIONS = "clusterwide-passivations";
   // activation interceptor
   public static final String ACTIVATIONS = "clusterwide-activations";
   public static final String CACHE_LOADER_LOADS = "clusterwide-cache-loader-loads";
   public static final String CACHE_LOADER_MISSES = "clusterwide-cache-loader-misses";
   public static final String CACHE_LOADER_STORES = "clusterwide-cache-loader-stores";

}
