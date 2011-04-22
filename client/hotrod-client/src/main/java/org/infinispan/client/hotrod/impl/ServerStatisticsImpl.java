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
package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.ServerStatistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ServerStatisticsImpl implements ServerStatistics {

   private Map<String, String> stats = new HashMap<String, String>();

   @Override
   public Map<String, String> getStatsMap() {
      return Collections.unmodifiableMap(stats);
   }

   @Override
   public String getStatistic(String statsName) {
      return stats.get(statsName);
   }

   public void addStats(String name, String value) {
      stats.put(name, value);
   }

   @Override
   public Integer getIntStatistic(String statsName) {
      String value = stats.get(statsName);
      return value == null ? null : Integer.parseInt(value); 
   }
}
