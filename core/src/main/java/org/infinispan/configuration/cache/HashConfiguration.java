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

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 */
public class HashConfiguration {

   private final ConsistentHash consistentHash;
   private final Hash hash;
   private final int numOwners;
   private final int numVirtualNodes;
   private final GroupsConfiguration groupsConfiguration;
   private final StateTransferConfiguration stateTransferConfiguration;
   // For use by the LegacyConfigurationAdapter
   final boolean activated;

   HashConfiguration(ConsistentHash consistentHash, Hash hash, int numOwners, int numVirtualNodes,
                     GroupsConfiguration groupsConfiguration, StateTransferConfiguration stateTransferConfiguration, boolean activated) {
      this.consistentHash = consistentHash;
      this.hash = hash;
      this.numOwners = numOwners;
      this.numVirtualNodes = numVirtualNodes;
      this.groupsConfiguration = groupsConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.activated = activated;
   }

   /**
    * The consistent hash in use.
    */
   public ConsistentHash consistentHash() {
      return consistentHash;
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
    * <p>
    * Controls the number of virtual nodes per "real" node. You can read more about virtual nodes in Infinispan's
    * <a href="https://docs.jboss.org/author/display/ISPN51">online user guide</a>.
    * </p>
    * 
    * <p>
    * If numVirtualNodes is 1, then virtual nodes are disabled. The topology aware consistent hash
    * must be used if you wish to take advnatage of virtual nodes.
    * </p>
    * 
    * <p>
    * A default of 1 is used.
    * </p>
    */
   public int numVirtualNodes() {
      return numVirtualNodes;
   }

   /**
    * If false, no rebalancing or rehashing will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} instead.
    */
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
            "activated=" + activated +
            ", consistentHash=" + consistentHash +
            ", hash=" + hash +
            ", numOwners=" + numOwners +
            ", numVirtualNodes=" + numVirtualNodes +
            ", groupsConfiguration=" + groupsConfiguration +
            ", stateTransferConfiguration=" + stateTransferConfiguration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HashConfiguration that = (HashConfiguration) o;

      if (activated != that.activated) return false;
      if (numOwners != that.numOwners) return false;
      if (numVirtualNodes != that.numVirtualNodes) return false;
      if (consistentHash != null ? !consistentHash.equals(that.consistentHash) : that.consistentHash != null)
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
      int result = consistentHash != null ? consistentHash.hashCode() : 0;
      result = 31 * result + (hash != null ? hash.hashCode() : 0);
      result = 31 * result + numOwners;
      result = 31 * result + numVirtualNodes;
      result = 31 * result + (groupsConfiguration != null ? groupsConfiguration.hashCode() : 0);
      result = 31 * result + (stateTransferConfiguration != null ? stateTransferConfiguration.hashCode() : 0);
      result = 31 * result + (activated ? 1 : 0);
      return result;
   }

}
