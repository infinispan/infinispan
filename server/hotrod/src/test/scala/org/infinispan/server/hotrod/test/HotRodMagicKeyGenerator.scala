package org.infinispan.server.hotrod.test

import org.infinispan.Cache
import java.util.Random
import org.infinispan.marshall.core.JBossMarshaller

/**
 * {@link org.infinispan.distribution.MagicKey} equivalent for HotRod
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
object HotRodMagicKeyGenerator {

   def newKey(cache: Cache[AnyRef, AnyRef]): Array[Byte] = {
      val ch = cache.getAdvancedCache.getDistributionManager.getReadConsistentHash
      val nodeAddress = cache.getAdvancedCache.getRpcManager.getAddress
      val r = new Random()

      val sm = new JBossMarshaller()
      for (i <- 0 to 1000) {
         val candidate = String.valueOf(r.nextLong())
         val candidateBytes = sm.objectToByteBuffer(candidate, 64)
         if (ch.isKeyLocalToNode(nodeAddress, candidateBytes)) {
            return candidateBytes
         }
      }

      throw new RuntimeException("Unable to find a key local to node " + cache)
   }

   def getStringObject(bytes: Array[Byte]): String = {
      val sm = new JBossMarshaller()
      sm.objectFromByteBuffer(bytes).asInstanceOf[String]
   }

}
