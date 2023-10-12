package org.infinispan.distribution.ch.impl;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.commons.hash.CRC16;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.util.Util;

/**
 * Implementation of {@link HashFunctionPartitioner} using {@link CRC16} for use with RESP.
 * Note that RESP only has 14 bits worth of slots and uses remainder based bucket allocation.
 * Due to this we have to treat it as having only 14 bits for a hash
 *
 * @since 15.0
 * @see HashFunctionPartitioner
 * @see <a href="https://redis.io/docs/reference/cluster-spec/#key-distribution-model">RESP key distribution</a>
 */
public class RESPHashFunctionPartitioner extends HashFunctionPartitioner {
   @Override
   public Hash getHash() {
      return CRC16.getInstance();
   }

   public static RESPHashFunctionPartitioner instance(int numSegments) {
      RESPHashFunctionPartitioner partitioner = new RESPHashFunctionPartitioner();
      partitioner.init(numSegments);
      return partitioner;
   }

   @Override
   protected void init(int numSegments) {
      if (!Util.isPow2(numSegments)) {
         throw CONFIG.respCacheSegmentSizePow2(numSegments);
      }
      super.init(numSegments);
   }

   @Override
   protected int bitsToUse() {
      // RESP uses only 14 out of the 16 bits from the CRC16 hash.
      // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
      return 14;
   }
}
