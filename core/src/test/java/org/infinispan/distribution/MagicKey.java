package org.infinispan.distribution;

import org.infinispan.Cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.hasOwners;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;

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

   /**
    * The name is used only for easier debugging and may be null. It is not part of equals()/hashCode().
    */
   private final String name;
   private final int hashcode;
   private final int segment;
   private final String address;

   public MagicKey(String name, Cache<?, ?> primaryOwner) {
      this.name = name;
      address = addressOf(primaryOwner).toString();
      Random r = new Random();
      Object dummy;
      int attemptsLeft = 100000;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Integer(hc);
         attemptsLeft--;

      } while (!isFirstOwner(primaryOwner, dummy) && attemptsLeft >= 0);

      if (attemptsLeft < 0) {
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner);
      }
      // we have found a hashcode that works!
      hashcode = dummy.hashCode();
      segment = primaryOwner.getAdvancedCache().getDistributionManager().getReadConsistentHash().getSegment(this);
   }

   public MagicKey(String name, Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      this.name = name;
      address = addressOf(primaryOwner).toString();
      Random r = new Random();
      Object dummy;
      int attemptsLeft = 1000;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Integer(hc);
         attemptsLeft--;

      } while (!hasOwners(dummy, primaryOwner, backupOwners) && attemptsLeft >= 0);

      if (attemptsLeft < 0) {
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner + ", "
               + Arrays.toString(backupOwners));
      }
      // we have found a hashcode that works!
      hashcode = dummy.hashCode();
      segment = primaryOwner.getAdvancedCache().getDistributionManager().getReadConsistentHash().getSegment(this);
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

      return hashcode == magicKey.hashcode && address.equals(magicKey.address);
   }

   @Override
   public String toString() {
      return "MagicKey#" + name + '{' + Integer.toHexString(hashcode) + '@' + address + '/' + segment + '}';
   }
}