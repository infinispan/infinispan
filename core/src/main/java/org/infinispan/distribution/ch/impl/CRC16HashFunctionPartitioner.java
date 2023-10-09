package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.CRC16;
import org.infinispan.commons.hash.Hash;

/**
 * Implementation of {@link HashFunctionPartitioner} using {@link CRC16}.
 *
 * @since 15.0
 * @see HashFunctionPartitioner
 */
public class CRC16HashFunctionPartitioner extends HashFunctionPartitioner {

   private boolean isPow2;

   public static CRC16HashFunctionPartitioner instance(int numSegments) {
      CRC16HashFunctionPartitioner partitioner = new CRC16HashFunctionPartitioner();
      partitioner.init(numSegments);
      return partitioner;
   }

   @Override
   protected void init(int numSegments) {
      super.init(numSegments);
      isPow2 = isPow2(numSegments);
   }

   public static boolean isPow2(int n) {
      // If `n` is pow2 the binary is a 1 followed by only zeroes and `n - 1` is a 0 followed by only ones.
      // The binary and always resolves to 0.
      return (n & (n - 1)) == 0;
   }

   @Override
   public int getSegment(Object key) {
      int h = hashObject(key);
      return segmentFromHash(h);
   }

   public int hashObject(Object key) {
      return hashFunction.hash(key) & Integer.MAX_VALUE;
   }

   public int segmentFromHash(int hash) {
      // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
      return isPow2 ? (hash & (numSegments - 1)) : hash % numSegments;
   }

   @Override
   public Hash getHash() {
      return CRC16.getInstance();
   }
}
