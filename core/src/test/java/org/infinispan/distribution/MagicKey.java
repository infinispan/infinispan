package org.infinispan.distribution;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.transport.Address;

/**
 * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that cache
 * (plus however many additional caches in the hash space).
 * <p>
 * Note that this only works if all the caches have joined a single cluster before creating the key.
 * If the cluster membership changes then the keys may move to other servers.
 */
public class MagicKey implements Serializable {

   private static final WeakHashMap<Integer, int[]> hashCodes = new WeakHashMap<>();
   private static final AtomicLong counter = new AtomicLong();

   /**
    * The name is used only for easier debugging and may be null. It is not part of equals()/hashCode().
    */
   @ProtoField(1)
   final String name;

   @ProtoField(number = 2, defaultValue = "0")
   final int hashcode;
   /**
    * As hash codes can collide, using counter makes the key unique.
    */
   @ProtoField(number = 3, defaultValue = "0")
   final long unique;

   @ProtoField(number = 4, defaultValue = "0")
   final int segment;

   @ProtoField(5)
   final String address;

   @ProtoFactory
   MagicKey(String name, int hashcode, long unique, int segment, String address) {
      this.name = name;
      this.hashcode = hashcode;
      this.unique = unique;
      this.segment = segment;
      this.address = address;
   }

   public MagicKey(String name, Cache<?, ?> primaryOwner) {
      this.name = name;
      Address primaryAddress = addressOf(primaryOwner);
      this.address = primaryAddress.toString();

      LocalizedCacheTopology cacheTopology = primaryOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      int segment = findSegment(ch.getNumSegments(), s -> primaryAddress.equals(ch.locatePrimaryOwnerForSegment(s)));
      if (segment < 0) {
         throw new IllegalStateException("Could not find any segment owned by " + primaryOwner +
               ", primary segments: " + segments(primaryOwner));
      }
      this.segment = segment;
      hashcode = getHashCodeForSegment(cacheTopology, segment);
      unique = counter.getAndIncrement();
   }

   public MagicKey(String name, Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      this.name = name;
      Address primaryAddress = addressOf(primaryOwner);
      this.address = primaryAddress.toString();

      LocalizedCacheTopology cacheTopology = primaryOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      segment = findSegment(ch.getNumSegments(), s -> {
         List<Address> owners = ch.locateOwnersForSegment(s);
         if (!primaryAddress.equals(owners.get(0))) return false;
         for (Cache<?, ?> backup : backupOwners) {
            if (!owners.contains(addressOf(backup))) return false;
         }
         return true;

      });
      if (segment < 0) {
         throw new IllegalStateException("Could not find any segment owned by " + primaryOwner + ", "
               + Arrays.toString(backupOwners) + ", primary segments: " + segments(primaryOwner)
               + ", backup segments: " + Stream.of(backupOwners).collect(Collectors.toMap(Function.identity(), this::segments)));
      }
      hashcode = getHashCodeForSegment(cacheTopology, segment);
      unique = counter.getAndIncrement();
   }

   private int findSegment(int numSegments, Predicate<Integer> predicate) {
      // use random offset so that we don't use only lower segments
      int offset = ThreadLocalRandom.current().nextInt(numSegments);
      for (int i = 0; i < numSegments; ++i) {
         int segment = (offset + i) % numSegments;
         if (predicate.test(segment)) {
            return segment;
         }
      }
      return -1;
   }

   private static synchronized int getHashCodeForSegment(LocalizedCacheTopology cacheTopology, int segment) {
      int numSegments = cacheTopology.getReadConsistentHash().getNumSegments();
      // Caching the hash codes prevents random failures in tests where we create many magic keys
      int[] hcs = hashCodes.computeIfAbsent(numSegments, k -> new int[numSegments]);
      int hc = hcs[segment];
      if (hc != 0) {
         return hc;
      }
      Random r = new Random();
      int attemptsLeft = 100 * numSegments;
      int dummy;
      do {
         dummy = r.nextInt();
         attemptsLeft--;
         if (attemptsLeft < 0) {
            throw new IllegalStateException("Could not find any key in segment " + segment);
         }
      } while (cacheTopology.getSegment(dummy) != segment);
      return hcs[segment] = dummy;
   }

   private Set<Integer> segments(Cache<?, ?> owner) {
      return owner.getAdvancedCache().getDistributionManager().getCacheTopology().getWriteConsistentHash()
            .getPrimarySegmentsForOwner(owner.getCacheManager().getAddress());
   }

   public MagicKey(Cache<?, ?> primaryOwner) {
      this(null, primaryOwner);
   }

   public MagicKey(Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      this(null, primaryOwner, backupOwners);
   }

   @Override
   public int hashCode() {
      return hashcode;
   }

   public int getSegment() {
      return segment;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MagicKey magicKey = (MagicKey) o;

      return hashcode == magicKey.hashcode && address.equals(magicKey.address) &&
            Objects.equals(name, magicKey.name) && unique == magicKey.unique;
   }

   @Override
   public String toString() {
      return String.format("MagicKey%s{%X/%08X/%d@%s}", name == null ? "" : "#" + name,
            unique, hashcode, segment, address);
   }
}
