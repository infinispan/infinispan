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
   val SuccessWithPrevious = Value(0x03)
   val NotExecutedWithPrevious = Value(0x04)

   val InvalidMagicOrMsgId = Value(0x81)
   val UnknownOperation = Value(0x82)
   val UnknownVersion = Value(0x83) // todo: test
   val ParseError = Value(0x84) // todo: test
   val ServerError = Value(0x85) // todo: test
   val OperationTimedOut = Value(0x86) // todo: test
   val NodeSuspected = Value(0x87)
   val IllegalLifecycleState = Value(0x88)

}
