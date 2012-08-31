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

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 */
public class HashConfiguration {

   private final ConsistentHashFactory consistentHashFactory;
   private final Hash hash;
   private final int numOwners;
   private final int numSegments;
   private final GroupsConfiguration groupsConfiguration;
   private final StateTransferConfiguration stateTransferConfiguration;

   HashConfiguration(ConsistentHashFactory consistentHashFactory, Hash hash, int numOwners, int numSegments,
                     GroupsConfiguration groupsConfiguration, StateTransferConfiguration stateTransferConfiguration) {
      this.consistentHashFactory = consistentHashFactory;
      this.hash = hash;
      this.numOwners = numOwners;
      this.numSegments = numSegments;
      this.groupsConfiguration = groupsConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
   }

   /**
    * @deprecated Since 5.2, replaced by {@link #consistentHashFactory()}.
    */
   @Deprecated
   public ConsistentHash consistentHash() {
      return null;
   }

   /**
    * The consistent hash factory in use.
    */
   public ConsistentHashFactory consistentHashFactory() {
       return consistentHashFactory;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator.
    * Typically one of the the many default {@link org.infinispan.distribution.ch.ConsistentHash}
    * implementations shipped.
    */
   public Hash hash() {
      return hash;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public int numOwners() {
      return numOwners;
   }

   /**
    * @deprecated No longer used since 5.2, replaced by {@link #numSegments()} (which works like a
    *    {@code numVirtualNodes} value for the entire cluster).
    */
   @Deprecated
   public int numVirtualNodes() {
      return 1;
   }

   /**
    * Controls the total number of hash space segments (per cluster).
    *
    * <p>A hash space segment is the granularity for key distribution in the cluster: a node can own
    * (or primary-own) one or more full segments, but not a fraction of a segment. As such, larger
    * {@code numSegments} values will mean a more even distribution of keys between nodes.
    * <p>On the other hand, the memory/bandwidth usage of the new consistent hash grows linearly with
    * {@code numSegments}. So we recommend keeping {@code numSegments <= 10 * clusterSize}.
    */
   public int numSegments() {
      return numSegments;
   }

   /**
    * If false, no rebalancing or rehashing will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} instead.
    */
   @Deprecated
   public boolean rehashEnabled() {
      return stateTransferConfiguration.fetchInMemoryState();
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   @Deprecated
   public long rehashRpcTimeout() {
      return stateTransferConfiguration.timeout();
   }

   /**
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   @Deprecated
   public long rehashWait() {
      return stateTransferConfiguration.timeout();
   }

   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }

   @Override
   public String toString() {
      return "HashConfiguration{" +
            "consistentHashFactory=" + consistentHashFactory +
            ", hash=" + hash +
            ", numOwners=" + numOwners +
            ", numSegments=" + numSegments +
            ", groupsConfiguration=" + groupsConfiguration +
            ", stateTransferConfiguration=" + stateTransferConfiguration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HashConfiguration that = (HashConfiguration) o;

      if (numOwners != that.numOwners) return false;
      if (numSegments != that.numSegments) return false;
      if (consistentHashFactory != null ? !consistentHashFactory.equals(that.consistentHashFactory) : that.consistentHashFactory != null)
         return false;
      if (groupsConfiguration != null ? !groupsConfiguration.equals(that.groupsConfiguration) : that.groupsConfiguration != null)
         return false;
      if (hash != null ? !hash.equals(that.hash) : that.hash != null)
         return false;
      if (stateTransferConfiguration != null ? !stateTransferConfiguration.equals(that.stateTransferConfiguration) : that.stateTransferConfiguration != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = consistentHashFactory != null ? consistentHashFactory.hashCode() : 0;
      result = 31 * result + (hash != null ? hash.hashCode() : 0);
      result = 31 * result + numOwners;
      result = 31 * result + numSegments;
      result = 31 * result + (groupsConfiguration != null ? groupsConfiguration.hashCode() : 0);
      result = 31 * result + (stateTransferConfiguration != null ? stateTransferConfiguration.hashCode() : 0);
      return result;
   }

}
