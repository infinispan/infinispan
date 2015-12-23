package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Key partitioner that computes a key's segment based on a hash function.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class HashFunctionPartitioner implements KeyPartitioner {
   private Hash hashFunction;
   private int numSegments;
   private int segmentSize;

   @Override
   public void init(HashConfiguration configuration) {
      Objects.requireNonNull(configuration);
      init(configuration.hash(), configuration.numSegments());
   }

   private void init(Hash hashFunction, int numSegments) {
      Objects.requireNonNull(hashFunction);
      if (numSegments <= 0) {
         throw new IllegalArgumentException("numSegments must be strictly positive");
      }
      this.hashFunction = hashFunction;
      this.numSegments = numSegments;
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   @Override
   public int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return (hashFunction.hash(key) & Integer.MAX_VALUE) / segmentSize;
   }

   public Hash getHash() {
      return hashFunction;
   }

   public List<Integer> getSegmentEndHashes() {
      List<Integer> hashes = new ArrayList<Integer>(numSegments);
      for (int i = 0; i < numSegments; i++) {
         hashes.add(((i + 1) % numSegments) * segmentSize);
      }
      return hashes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      HashFunctionPartitioner that = (HashFunctionPartitioner) o;

      if (numSegments != that.numSegments)
         return false;
      return Objects.equals(hashFunction, that.hashFunction);

   }

   @Override
   public int hashCode() {
      int result = hashFunction != null ? hashFunction.hashCode() : 0;
      result = 31 * result + numSegments;
      return result;
   }

   public static class Externalizer extends NoStateExternalizer<HashFunctionPartitioner> {
      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends HashFunctionPartitioner>> getTypeClasses() {
         return Util.<Class<? extends HashFunctionPartitioner>>asSet(HashFunctionPartitioner.class);
      }

      @Override
      public HashFunctionPartitioner readObject(ObjectInput input)
            throws IOException, ClassNotFoundException {
         return new HashFunctionPartitioner();
      }

      @Override
      public Integer getId() {
         return Ids.HASH_FUNCTION_PARTITIONER;
      }
   }
}
