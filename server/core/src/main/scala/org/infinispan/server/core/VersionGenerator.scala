package org.infinispan.server.core

import org.infinispan.remoting.transport.Address
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.collection.JavaConversions._

/**
 * This class generates version numbers to be stored with cache values whenever a value is created or modified.
 * This version can later be queried by clients and used to guarantee that modifications are atomic.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object VersionGenerator {

   // TODO: Possibly seed version counter on capped System.currentTimeMillis, to avoid issues with clients holding to versions in between restarts
   private val versionCounter = new AtomicInteger

   private val versionPrefix = new AtomicLong

   def newVersion(isClustered: Boolean): Long = {
      if (isClustered && versionPrefix.get == 0)
         throw new IllegalStateException("If clustered, Version prefix cannot be 0. Rank calculator probably not in use.")
      val counter = versionCounter.incrementAndGet
      // Version counter occupies the least significant 4 bytes of the version
      if (isClustered) versionPrefix.get | counter else counter
   }

   def getRankCalculatorListener = RankCalculator

   private[core] def resetCounter {
      versionCounter.compareAndSet(versionCounter.get, 0)
   }

   private def findAddressRank(address: Address, members: Iterable[Address], rank: Int): Int = {
      if (address.equals(members.head)) rank
      else findAddressRank(address, members.tail, rank + 1)
   }

   @Listener
   object RankCalculator extends Logging {
      @ViewChanged
      def calculateRank(e: ViewChangedEvent) {
         val rank = calculateRank(e.getLocalAddress, asIterable(e.getNewMembers), e.getViewId)
         if (isTraceEnabled) trace("Calculated rank based on view %s and result was %d", e, rank)
      }

      private[core] def calculateRank(address: Address, members: Iterable[Address], viewId: Long): Long = {
         val rank: Long = findAddressRank(address, members, 1)
         // Version is composed of: <view id (2 bytes)><rank (2 bytes)><version counter (4 bytes)>
         // View id and rank form the prefix which is updated on a view change.
         val newVersionPrefix = (viewId << 48) | (rank << 32)
         // TODO: Deal with compareAndSet failures?
         versionPrefix.compareAndSet(versionPrefix.get, newVersionPrefix)
         versionPrefix.get
      }
   }
}
