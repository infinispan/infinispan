package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class ErrorResponse(override val id: Long,
                    override val opCode: OpCodes.OpCode,
                    override val status: Status.Status,
                    val msg: String) extends Response(id, opCode, status) {

   override def toString = {
      new StringBuilder().append("ErrorResponse").append("{")
         .append("id=").append(id)
         .append(", opCode=").append(opCode)
         .append(", status=").append(status)
         .append(", msg=").append(msg)
         .append("}").toString
   }
   
}