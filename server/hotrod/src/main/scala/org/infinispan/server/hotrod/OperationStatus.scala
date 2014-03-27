package org.infinispan.server.hotrod

/**
 * Hot Rod operation possible status outcomes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object OperationStatus extends Enumeration {
   type OperationStatus = Value

   val Success = Value(0x00)
   val OperationNotExecuted = Value(0x01)
   val KeyDoesNotExist = Value(0x02)

   val InvalidMagicOrMsgId = Value(0x81)
   val UnknownOperation = Value(0x82)
   val UnknownVersion = Value(0x83)
   val ParseError = Value(0x84)
   val ServerError = Value(0x85)
   val OperationTimedOut = Value(0x86)

}
