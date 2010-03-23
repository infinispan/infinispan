package org.infinispan.server.core

import org.infinispan.remoting.transport.Address
import org.infinispan.Cache
import java.util.concurrent.atomic.AtomicInteger

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object VersionGenerator {

   private val versionCounter = new AtomicInteger

   def newVersion(address: Option[Address], members: Option[Iterable[Address]], viewId: Long): Long = {
      val counter = versionCounter.incrementAndGet
      var version: Long = counter
      if (address != None && members != None) {
         // todo: perf tip: cache rank and viewId as a volatile and recalculate with each view change
         val rank: Long = findAddressRank(address.get, members.get, 1)
         version = (rank << 32) | version
         version = (viewId << 48) | version
      }
      version
   }

   private def findAddressRank(address: Address, members: Iterable[Address], rank: Int): Int = {
      if (address.equals(members.head)) rank
      else findAddressRank(address, members.tail, rank + 1)
   }
   
}