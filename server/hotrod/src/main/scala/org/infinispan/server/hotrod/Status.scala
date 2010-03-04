package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

object Status extends Enumeration {
   type Status = Value

   val Success = Value(0x00)
   val KeyDoesNotExist = Value(0x02)
}