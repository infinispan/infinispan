package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

object OpCodes extends Enumeration {
   type OpCode = Value

   val PutRequest = Value(0x01)
   val PutResponse = Value(0x02)
   val GetRequest = Value(0x03)
   val GetResponse = Value(0x04)

   val ErrorResponse = Value(0x50)
}