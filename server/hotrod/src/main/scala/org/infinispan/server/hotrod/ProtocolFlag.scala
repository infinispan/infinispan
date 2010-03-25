package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object ProtocolFlag extends Enumeration {
   type ProtocolFlag = Value
   val NoFlag = Value(0)
   val ForceReturnValue = Value(1)
}