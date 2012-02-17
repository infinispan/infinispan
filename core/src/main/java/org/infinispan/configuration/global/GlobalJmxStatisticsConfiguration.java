/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.util.TypedProperties;

public class GlobalJmxStatisticsConfiguration {

   private final boolean enabled;
   private final String jmxDomain;
   private final MBeanServerLookup mBeanServerLookup;
   private final boolean allowDuplicateDomains;
   private final String cacheManagerName;
   private final TypedProperties properties;
   
   GlobalJmxStatisticsConfiguration(boolean enabled, String jmxDomain, MBeanServerLookup mBeanServerLookup,
         boolean allowDuplicateDomains, String cacheManagerName, TypedProperties properties) {
      this.enabled = enabled;
      this.jmxDomain = jmxDomain;
      this.mBeanServerLookup = mBeanServerLookup;
      this.allowDuplicateDomains = allowDuplicateDomains;
      this.cacheManagerName = cacheManagerName;
      this.properties = properties;
   }

   public boolean enabled() {
      return enabled;
   }

   public String domain() {
      return jmxDomain;
   }
   
   public TypedProperties properties() {
      return properties;
   }

   public boolean allowDuplicateDomains() {
      return allowDuplicateDomains;
   }

   public String cacheManagerName() {
      return cacheManagerName;
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup;
   }

   @Override
   public String toString() {
      return "GlobalJmxStatisticsConfiguration{" +
            "allowDuplicateDomains=" + allowDuplicateDomains +
            ", enabled=" + enabled +
            ", jmxDomain='" + jmxDomain + '\'' +
            ", mBeanServerLookup=" + mBeanServerLookup +
            ", cacheManagerName='" + cacheManagerName + '\'' +
            ", properties=" + properties +
            '}';
   }

}