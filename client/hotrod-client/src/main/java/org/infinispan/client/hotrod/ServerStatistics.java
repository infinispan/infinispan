/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod;

import java.util.Map;

/**
 * Defines the possible list of statistics defined by the Hot Rod server.
 * Can be obtained through {@link RemoteCache#stats()}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface ServerStatistics {

   /**
    * Number of seconds since Hot Rod started.
    */
   public static final String TIME_SINCE_START = "timeSinceStart";

   /**
    * Number of entries currently in the Hot Rod server
    */
   public static final String CURRENT_NR_OF_ENTRIES = "currentNumberOfEntries";

   /**
    * Number of entries stored in Hot Rod server.
    */
   public static final String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

   /**
    * Number of put operations.
    */
   public static final String STORES = "stores";


   /**
    * Number of get operations.
    */
   public static final String RETRIEVALS = "retrievals";

   /**
    * Number of get hits.
    */
   public static final String HITS = "hits";

   /**
    * Number of get misses.
    */
   public static final String MISSES = "misses";


   /**
    * Number of removal hits.
    */
   public static final String REMOVE_HITS = "removeHits";

   /**
    * Number of removal misses.
    */
   public static final String REMOVE_MISSES = "removeMisses";

   public Map<String, String> getStatsMap();

   public String getStatistic(String statsName);

   public Integer getIntStatistic(String statsName);
}
