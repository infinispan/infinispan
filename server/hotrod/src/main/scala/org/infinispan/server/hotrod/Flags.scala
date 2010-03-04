package org.infinispan.server.hotrod

import scala.collection.mutable.HashSet
import scala.collection.immutable
import org.infinispan.context.Flag

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

object Flags extends Enumeration {

   private val ZeroLockAcquisitionTimeout = Value(1, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT.toString)
   private val CacheModeLocal = Value(1 << 1, Flag.CACHE_MODE_LOCAL.toString)
   private val SkipLocking = Value(1 << 2, Flag.SKIP_LOCKING.toString)
   private val ForceWriteLock = Value(1 << 3, Flag.FORCE_WRITE_LOCK.toString)
   private val SkipCacheStatusCheck = Value(1 << 4, Flag.SKIP_CACHE_STATUS_CHECK.toString)
   private val ForceAsynchronous = Value(1 << 5, Flag.FORCE_ASYNCHRONOUS.toString)
   private val ForceSynchronous = Value(1 << 6, Flag.FORCE_SYNCHRONOUS.toString)
   // 1 << 7 skipped since that's the variable length marker
   private val SkipCacheStore = Value(1 << 8, Flag.SKIP_CACHE_STORE.toString)
   private val FailSilently = Value(1 << 9, Flag.FAIL_SILENTLY.toString)
   private val SkipRemoteLookup = Value(1 << 10, Flag.SKIP_REMOTE_LOOKUP.toString)
   private val PutForExternalRead = Value(1 << 11, Flag.PUT_FOR_EXTERNAL_READ.toString)

   def extract(bitFlags: Int): Set[Flag] = {
      val s = new HashSet[Flag]
      Flags.values.filter(f => (bitFlags & f.id) > 0).foreach(f => s += Flag.valueOf(f.toString))
      new immutable.HashSet ++ s
   }

}