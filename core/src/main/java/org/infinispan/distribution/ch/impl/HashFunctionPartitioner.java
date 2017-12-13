package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;

/**
 * Key partitioner that computes a key's segment based on a hash function.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class HashFunctionPartitioner implements KeyPartitioner, Cloneable {
   private Hash hashFunction;
   private int numSegments;
   private int segmentSize;


   public HashFunctionPartitioner() {}

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
      List<Integer> hashes = new ArrayList<>(numSegments);
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

   @Override
   public String toString() {
      return "HashFunctionPartitioner{" +
            "hashFunction=" + hashFunction +
            ", ns=" + numSegments +
            '}';
   }
}
