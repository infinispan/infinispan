package org.infinispan.distribution.ch.impl;

import java.io.ObjectInput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link SyncConsistentHashFactory} adapted for replicated caches, so that the primary owner of a key
 * is the same in replicated and distributed caches.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class SyncReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {
   private static final Log log = LogFactory.getLog(SyncReplicatedConsistentHashFactory.class);
   public static final float OWNED_SEGMENTS_ALLOWED_VARIATION = 1.10f;
   public static final float PRIMARY_SEGMENTS_ALLOWED_VARIATION = 1.20f;

   private static final SyncConsistentHashFactory syncCHF = new SyncConsistentHashFactory();

   @Override
   public ReplicatedConsistentHash create(Hash hashFunction, int numOwners, int numSegments,
         List<Address> members, Map<Address, Float> capacityFactors) {
      DefaultConsistentHash dch = syncCHF.create(hashFunction, 1, numSegments, members, null);
      return replicatedFromDefault(dch);
   }

   @Override
   public ReplicatedConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!ReplicatedConsistentHash.class.getName().equals(consistentHashClass))
         throw log.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new ReplicatedConsistentHash(state);
   }

   private ReplicatedConsistentHash replicatedFromDefault(DefaultConsistentHash dch) {
      int numSegments = dch.getNumSegments();
      List<Address> members = dch.getMembers();
      int[] primaryOwners = new int[numSegments];
      for (int segment = 0; segment < numSegments; segment++) {
         primaryOwners[segment] = members.indexOf(dch.locatePrimaryOwnerForSegment(segment));
      }
      return new ReplicatedConsistentHash(dch.getHashFunction(), members, primaryOwners);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers,
         Map<Address, Float> actualCapacityFactors) {
      DefaultConsistentHash baseDCH = defaultFromReplicated(baseCH);
      DefaultConsistentHash dch = syncCHF.updateMembers(baseDCH, newMembers, null);
      return replicatedFromDefault(dch);
   }

   private DefaultConsistentHash defaultFromReplicated(ReplicatedConsistentHash baseCH) {
      int numSegments = baseCH.getNumSegments();
      List<Address>[] baseSegmentOwners = new List[numSegments];
      for (int segment = 0; segment < numSegments; segment++) {
         baseSegmentOwners[segment] = Collections.singletonList(baseCH.locatePrimaryOwnerForSegment(segment));
      }
      return new DefaultConsistentHash(baseCH.getHashFunction(), 1,
            numSegments, baseCH.getMembers(), null, baseSegmentOwners);
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      DefaultConsistentHash baseDCH = defaultFromReplicated(baseCH);
      DefaultConsistentHash dch = syncCHF.rebalance(baseDCH);
      return replicatedFromDefault(dch);
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      return ch1.union(ch2);
   }

   public static class Externalizer extends AbstractExternalizer<SyncReplicatedConsistentHashFactory> {

      @Override
      public void writeObject(UserObjectOutput output, SyncReplicatedConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public SyncReplicatedConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new SyncReplicatedConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SYNC_REPLICATED_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends SyncReplicatedConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends SyncReplicatedConsistentHashFactory>>singleton(
               SyncReplicatedConsistentHashFactory.class);
      }
   }
}
