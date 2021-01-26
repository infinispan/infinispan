package org.infinispan.commons.hash;

import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;
import net.openhft.hashing.LongHashFunction;

/**
 * MurmurHash3 that delegates to OpenHFT - Zero-Allocation-Hashing
 */
@ThreadSafe
@Immutable
public class MurmurHash3 implements Hash {
   private final static MurmurHash3 instance = new MurmurHash3();
   private final static LongHashFunction hashFn = LongHashFunction.murmur_3();
   public static MurmurHash3 getInstance() {
      return instance;
   }

   private MurmurHash3() {
   }

   public static int hash(byte[] payload, int seed) {
      return (int) LongHashFunction.murmur_3(seed).hashBytes(payload);
   }

   public static int hash(long[] payload, int seed) {
      return (int) LongHashFunction.murmur_3(seed).hashLongs(payload);
   }

   @Override
   public int hash(byte[] payload) {
      // todo - prevent casting by changing the interface to long if the benchmark looks good
      return (int) hashFn.hashBytes(payload);
   }

   /**
    * Hashes a byte array efficiently.
    *
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   public static int hash(long[] payload) {
      return (int) hashFn.hashLongs(payload);
   }

   @Override
   public int hash(int hashcode) {
      return (int) hashFn.hashInt(hashcode);
   }

   @Override
   public int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof WrappedBytes) {
         return hash(((WrappedBytes) o).getBytes());
      }
      else if (o instanceof long[])
         return hash((long[]) o);
      else if (o instanceof String)
         return hashString((String) o);
      else
         return hash(o.hashCode());
   }

   private int hashString(String hashcode) {
      return (int) hashFn.hashChars(hashcode);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 0;
   }

   @Override
   public String toString() {
      return "MurmurHash3";
   }

   public static class Externalizer extends NoStateExternalizer<MurmurHash3> {
      @Override
      public Set<Class<? extends MurmurHash3>> getTypeClasses() {
         return Collections.singleton(MurmurHash3.class);
      }

      @Override
      public MurmurHash3 readObject(ObjectInput input) {
         return instance;
      }

      @Override
      public Integer getId() {
         return Ids.MURMURHASH_3;
      }
   }
}
