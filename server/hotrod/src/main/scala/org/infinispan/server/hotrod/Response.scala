package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class Response(val id: Long,
               val opCode: OpCodes.OpCode,
               val status: Status.Status) {

   override def toString = {
      new StringBuilder().append("Response").append("{")
         .append("id=").append(id)
         .append(", opCode=").append(opCode)
         .append(", status=").append(status)
         .append("}").toString
   }

}