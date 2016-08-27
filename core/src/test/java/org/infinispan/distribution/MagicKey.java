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
import org.infinispan.remoting.transport.Address;

/**
 * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that cache
 * (plus however many additional caches in the hash space).
 *
 * Note that this only works if all the caches have joined a single cluster before creating the key.
 * If the cluster membership changes then the keys may move to other servers.
 */
public class MagicKey implements Serializable {
   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -835275755945753954L;

   private static final WeakHashMap<ConsistentHash, int[]> hashCodes = new WeakHashMap<>();
   private static final AtomicLong counter = new AtomicLong();

   /**
    * The name is used only for easier debugging and may be null. It is not part of equals()/hashCode().
    */
   private final String name;
   private final int hashcode;
   /**
    * As hashcodes can collide, using counter makes the key unique.
    */
   private final long unique;
   private final int segment;
   private final String address;

   public MagicKey(String name, Cache<?, ?> primaryOwner) {
      this.name = name;
      Address primaryAddress = addressOf(primaryOwner);
      this.address = primaryAddress.toString();

      ConsistentHash ch = primaryOwner.getAdvancedCache().getDistributionManager().getConsistentHash();
      int segment = findSegment(ch.getNumSegments(), s -> primaryAddress.equals(ch.locatePrimaryOwnerForSegment(s)));
      if (segment < 0) {
         throw new IllegalStateException("Could not find any segment owned by " + primaryOwner +
            ", primary segments: " + segments(primaryOwner));
      }
      this.segment = segment;
      hashcode = getHashCodeForSegment(ch, segment);
      unique = counter.getAndIncrement();
   }

   public MagicKey(String name, Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      this.name = name;
      Address primaryAddress = addressOf(primaryOwner);
      this.address = primaryAddress.toString();

      ConsistentHash ch = primaryOwner.getAdvancedCache().getDistributionManager().getConsistentHash();
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
            + ", backup segments: " + Stream.of(backupOwners).collect(Collectors.toMap(Function.identity(), owner -> segments(owner))));
      }
      hashcode = getHashCodeForSegment(ch, segment);
      unique = counter.getAndIncrement();
   }

   public int findSegment(int numSegments, Predicate<Integer> predicate) {
      // use random offset so that we don't use only lower segments
      int offset = ThreadLocalRandom.current().nextInt(numSegments);
      int segment = 0;
      for (int i = 0; i < numSegments; ++i) {
         segment = (offset + i) % numSegments;
         if (predicate.test(segment)) {
            return segment;
         }
      }
      return -1;
   }

   private static synchronized int getHashCodeForSegment(ConsistentHash ch, int segment) {
      // Caching the hashcodes prevents random failures in tests where we create many magic keys
      int[] hcs = hashCodes.get(ch);
      if (hcs == null) {
         hashCodes.put(ch, hcs = new int[ch.getNumSegments()]);
      }
      int hc = hcs[segment];
      if (hc != 0) {
         return hc;
      }
      Random r = new Random();
      int attemptsLeft = 100 * ch.getNumSegments();
      Integer dummy;
      do {
         dummy = r.nextInt();
         attemptsLeft--;
         if (attemptsLeft < 0) {
            throw new IllegalStateException("Could not find any key in segment " + segment);
         }
      } while (ch.getSegment(dummy) != segment);
      return hcs[segment] = dummy.intValue();
   }

   private Set<Integer> segments(Cache<?, ?> owner) {
      return owner.getAdvancedCache().getDistributionManager().getConsistentHash().getPrimarySegmentsForOwner(owner.getCacheManager().getAddress());
   }

   public MagicKey(Cache<?, ?> primaryOwner) {
      this(null, primaryOwner);
   }

   public MagicKey(Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      this(null, primaryOwner, backupOwners);
   }

   @Override
   public int hashCode () {
      return hashcode;
   }

   @Override
   public boolean equals (Object o) {
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
