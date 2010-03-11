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

   val InvalidMagicOrMsgId = Value(0x81)
   val UnknownCommand = Value(0x82)
   val UnknownVersion = Value(0x83) // todo: test
   val ParseError = Value(0x84) // todo: test
   val ServerError = Value(0x85) // todo: test
   val CommandTimedOut = Value(0x86) // todo: test
   
}