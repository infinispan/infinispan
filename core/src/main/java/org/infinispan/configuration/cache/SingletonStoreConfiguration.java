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
 * SingletonStore is a delegating cache store used for situations when only one instance in a
 * cluster should interact with the underlying store. The coordinator of the cluster will be
 * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore
 * implementation. It always delegates reads to the real CacheStore.
 * 
 * @author pmuir
 * 
 */
public class SingletonStoreConfiguration {

   private final boolean enabled;
   private final long pushStateTimeout;
   private final boolean pushStateWhenCoordinator;

   SingletonStoreConfiguration(boolean enabled, long pushStateTimeout, boolean pushStateWhenCoordinator) {
      this.enabled = enabled;
      this.pushStateTimeout = pushStateTimeout;
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   /**
    * If true, the singleton store cache store is enabled.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public long pushStateTimeout() {
      return pushStateTimeout;
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    */
   public boolean pushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   @Override
   public String toString() {
      return "SingletonStoreConfiguration{" +
            "enabled=" + enabled +
            ", pushStateTimeout=" + pushStateTimeout +
            ", pushStateWhenCoordinator=" + pushStateWhenCoordinator +
            '}';
   }

}
