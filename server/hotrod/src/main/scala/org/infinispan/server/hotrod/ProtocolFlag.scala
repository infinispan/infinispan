package org.infinispan.server.hotrod

/**
 * @author Galder Zamarreño
 */
object ProtocolFlag extends Enumeration {
   type ProtocolFlag = Enumeration#Value
   val NoFlag = Value
   val ForceReturnPreviousValue = Value(0x01)
   val DefaultLifespan = Value(0x02)
   val DefaultMaxIdle = Value(0x04)
   val SkipCacheLoader = Value(0x08)
   val SkipIndexing = Value(0x10)
}
