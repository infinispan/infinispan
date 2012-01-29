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
 * Defines clustered characteristics of the cache.
 * 
 * @author pmuir
 * 
 */
public class ClusteringConfiguration {

   private final CacheMode cacheMode;
   private final AsyncConfiguration asyncConfiguration;
   private final HashConfiguration hashConfiguration;
   private final L1Configuration l1Configuration;
   private final StateTransferConfiguration stateTransferConfiguration;
   private final SyncConfiguration syncConfiguration;

   ClusteringConfiguration(CacheMode cacheMode, AsyncConfiguration asyncConfiguration, HashConfiguration hashConfiguration,
         L1Configuration l1Configuration, StateTransferConfiguration stateTransferConfiguration, SyncConfiguration syncConfiguration) {
      this.cacheMode = cacheMode;
      this.asyncConfiguration = asyncConfiguration;
      this.hashConfiguration = hashConfiguration;
      this.l1Configuration = l1Configuration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.syncConfiguration = syncConfiguration;
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public CacheMode cacheMode() {
      return cacheMode;
   }

   public String cacheModeString() {
      return cacheMode == null ? "none" : cacheMode.toString();
   }

   /**
    * Configure async sub element. Once this method is invoked users cannot subsequently invoke
    * <code>sync()</code> as two are mutually exclusive
    */
   public AsyncConfiguration async() {
      return asyncConfiguration;
   }

   /**
    * Configure hash sub element
    */
   public HashConfiguration hash() {
      return hashConfiguration;
   }

   /**
    * This method allows configuration of the L1 cache for distributed caches. When this method is
    * called, it automatically enables L1. So, if you want it to be disabled, make sure you call
    * {@link org.infinispan.configuration.cache.L1ConfigurationBuilder#disable()}
    */
   public L1Configuration l1() {
      return l1Configuration;
   }

   /**
    * Configure sync sub element. Once this method is invoked users cannot subsequently invoke
    * <code>async()</code> as two are mutually exclusive
    */
   public SyncConfiguration sync() {
      return syncConfiguration;
   }

   public StateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }

}
