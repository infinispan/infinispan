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
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.ConfigurationException;
import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 * 
 */
public class HashConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<HashConfiguration> {

   private ConsistentHash consistentHash;
   private Hash hash = new MurmurHash3();
   private int numOwners = 2;
   private int numVirtualNodes = 1;
   private boolean activated = false;

   private final GroupsConfigurationBuilder groupsConfigurationBuilder;

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.groupsConfigurationBuilder = new GroupsConfigurationBuilder(builder);
   }

   /**
    * The consistent hash in use.
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    */
   public HashConfigurationBuilder consistentHash(ConsistentHash consistentHash) {
      this.consistentHash = consistentHash;
      activated = true;
      return this;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public HashConfigurationBuilder numOwners(int numOwners) {
      if (numVirtualNodes < 1) throw new IllegalArgumentException("numOwners cannot be less than 1");
      this.numOwners = numOwners;
      activated = true;
      return this;
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
    *
    * @param numVirtualNodes the number of virtual nodes. Must be &gt; 0.
    * @throws IllegalArgumentException if numVirtualNodes &lt; 1
    */
   public HashConfigurationBuilder numVirtualNodes(int numVirtualNodes) {
      if (numVirtualNodes < 1) throw new IllegalArgumentException("numVirtualNodes cannot be less than 1");
      this.numVirtualNodes = numVirtualNodes;
      activated = true;
      return this;
   }

   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashEnabled() {
      stateTransfer().fetchInMemoryState(true);
      activated = true;
      return this;
   }
   
   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashEnabled(boolean enabled) {
      stateTransfer().fetchInMemoryState(enabled);
      activated = true;
      return this;
   }

   /**
    * Disable rebalancing and rehashing, which would have taken place when a new node joins the
    * cluster or a node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashDisabled() {
      stateTransfer().fetchInMemoryState(false);
      activated = true;
      return this;
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link StateTransferConfigurationBuilder#timeout(long)} instead.
    */
   @Deprecated
   public HashConfigurationBuilder rehashRpcTimeout(long rehashRpcTimeout) {
      stateTransfer().timeout(rehashRpcTimeout);
      activated = true;
      return this;
   }

   /**
    * @deprecated No longer used.
    */
   @Deprecated
   public HashConfigurationBuilder rehashWait(long rehashWait) {
      return this;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator. Typically
    * used in conjunction with the many default
    * {@link org.infinispan.distribution.ch.ConsistentHash} implementations shipped.
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    */
   public HashConfigurationBuilder hash(Hash hash) {
      this.hash = hash;
      activated = true;
      return this;
   }

   public GroupsConfigurationBuilder groups() {
      activated = true;
      return groupsConfigurationBuilder;
   }

   @Override
   void validate() {
      if (activated && !clustering().cacheMode().isDistributed())
         throw new ConfigurationException("Configuring the hashing behavior of entries is only supported when using DISTRIBUTED as a cache mode.  Your cache mode is set to " + clustering().cacheMode().friendlyCacheModeString());
      groupsConfigurationBuilder.validate();
   }

   @Override
   HashConfiguration create() {
      // TODO stateTransfer().create() will create a duplicate StateTransferConfiguration instance
      return new HashConfiguration(consistentHash, hash, numOwners, numVirtualNodes,
            groupsConfigurationBuilder.create(), stateTransfer().create(), activated);
   }

   @Override
   public HashConfigurationBuilder read(HashConfiguration template) {
      this.consistentHash = template.consistentHash();
      this.hash = template.hash();
      this.numOwners = template.numOwners();
      this.numVirtualNodes = template.numVirtualNodes();
      this.groupsConfigurationBuilder.read(template.groups());

      return this;
   }
   
}
