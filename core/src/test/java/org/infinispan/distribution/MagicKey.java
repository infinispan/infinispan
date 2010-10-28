package org.infinispan.distribution;

import org.infinispan.Cache;

import java.io.Serializable;
import java.util.Random;

import static org.infinispan.distribution.BaseDistFunctionalTest.addressOf;
import static org.infinispan.distribution.BaseDistFunctionalTest.isFirstOwner;

/**
 * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that cache
 * (plus however many additional caches in the hash space)
 */
public class MagicKey implements Serializable {
   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -835275755945753954L;
   String name = null;
   int hashcode;
   String address;

   public MagicKey(Cache<?, ?> toMapTo) {
      address = addressOf(toMapTo).toString();
      Random r = new Random();
      Object dummy;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Object() {
            @Override
            public int hashCode() {
               return hc;
            }
         };

      } while (!isFirstOwner(toMapTo, dummy));

      // we have found a hashcode that works!
      hashcode = dummy.hashCode();
   }

   public MagicKey(Cache<?, ?> toMapTo, String name) {
      this(toMapTo);
      this.name = name;
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

      if (hashcode != magicKey.hashcode) return false;
      if (address != null ? !address.equals(magicKey.address) : magicKey.address != null) return false;

      return true;
   }

   @Override
   public String toString() {
      return "MagicKey{" +
              (name == null ? "" : "name=" + name + ", ") +
              "hashcode=" + hashcode +
              ", address='" + address + '\'' +
              '}';
   }
}