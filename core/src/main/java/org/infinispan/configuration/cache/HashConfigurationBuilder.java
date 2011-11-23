package org.infinispan.configuration.cache;

import static java.util.concurrent.TimeUnit.MINUTES;

import org.infinispan.commons.util.hash.Hash;
import org.infinispan.commons.util.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultHashSeed;
import org.infinispan.distribution.ch.HashSeed;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 * 
 */
public class HashConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<HashConfiguration> {

   private ConsistentHash consistentHash;
   private Hash hash = new MurmurHash3();
   private HashSeed hashSeed = new DefaultHashSeed();
   private int numOwners = 2;
   private int numVirtualNodes = 1;
   private boolean rehashEnabled = true;
   private long rehashRpcTimeout = MINUTES.toMillis(10);
   private long rehashWait = MINUTES.toMillis(1);

   private final GroupsConfigurationBuilder groupsConfigurationBuilder;

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.groupsConfigurationBuilder = new GroupsConfigurationBuilder(builder);
   }

   /**
    * The consistent hash in use.
    */
   public HashConfigurationBuilder consistentHash(ConsistentHash consistentHash) {
      this.consistentHash = consistentHash;
      return this;
   }

   /**
    * A hash seed implementation which allows seed address to for consistent hash calculation to be
    * configured. This is particularly useful when Infinispan is accessed remotely and clients are
    * to calculate hash ids. Since clients are only aware of server endpoints, implementations of
    * {@link HashSeed} can seed based on this information instead of the traditional cluster
    * address.
    */
   public HashConfigurationBuilder hashSeed(HashSeed hashSeed) {
      this.hashSeed = hashSeed;
      return this;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public HashConfigurationBuilder numOwners(int numOwners) {
      this.numOwners = numOwners;
      return this;
   }

   /**
    * <p>
    * Controls the number of virtual nodes per "real" node. You can read more about virtual nodes at
    * TODO
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
    * @param numVirtualNodes the number of virtual nodes. Must be >0.
    * @throws IllegalArgumentException if numVirtualNodes <0
    */
   public HashConfigurationBuilder numVirtualNodes(int numVirtualNodes) {
      this.numVirtualNodes = numVirtualNodes;
      return this;
   }

   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    */
   public HashConfigurationBuilder rehashEnabled() {
      this.rehashEnabled = true;
      return this;
   }

   /**
    * Disable rebalancing and rehashing, which would have taken place when a new node joins the
    * cluster or a node leaves
    */
   public HashConfigurationBuilder rehashDisabled() {
      this.rehashEnabled = false;
      return this;
   }

   /**
    * Rehashing timeout
    */
   public HashConfigurationBuilder rehashRpcTimeout(long rehashRpcTimeout) {
      this.rehashRpcTimeout = rehashRpcTimeout;
      return this;
   }

   /**
    * 
    */
   public HashConfigurationBuilder rehashWait(long rehashWait) {
      this.rehashWait = rehashWait;
      return this;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator. Typically
    * used in conjunction with the many default
    * {@link org.infinispan.distribution.ch.ConsistentHash} implementations shipped.
    */
   public HashConfigurationBuilder hash(Hash hash) {
      this.hash = hash;
      return this;
   }

   public GroupsConfigurationBuilder groups() {
      return groupsConfigurationBuilder;
   }

   @Override
   void validate() {
      groupsConfigurationBuilder.validate();
   }

   @Override
   HashConfiguration create() {
      return new HashConfiguration(consistentHash, hashSeed, hash, numOwners, numVirtualNodes, rehashEnabled, rehashRpcTimeout,
            rehashWait, groupsConfigurationBuilder.create());
   }

}
