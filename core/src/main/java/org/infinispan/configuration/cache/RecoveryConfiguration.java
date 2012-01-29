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
package org.infinispan.configuration.cache;

/**
 * Defines recovery configuration for the cache.
 * 
 * @author pmuir
 * 
 */
public class RecoveryConfiguration {

   public static final String DEFAULT_RECOVERY_INFO_CACHE = "__recoveryInfoCacheName__";

   private final boolean enabled;
   private final String recoveryInfoCacheName;

   RecoveryConfiguration(boolean enabled, String recoveryInfoCacheName) {
      this.enabled = enabled;
      this.recoveryInfoCacheName = recoveryInfoCacheName;
   }

   /**
    * Determines if recovery is enabled for the cache.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
   public String recoveryInfoCacheName() {
      return recoveryInfoCacheName;
   }

}
