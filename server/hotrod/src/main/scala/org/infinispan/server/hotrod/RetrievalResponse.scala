package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class RetrievalResponse(override val id: Long,
                        override val opCode: OpCodes.OpCode,
                        override val status: Status.Status,
                        val value: Array[Byte]) extends Response(id, opCode, status) {

   override def toString = {
      new StringBuilder().append("RetrievalResponse").append("{")
         .append("opCode=").append(opCode)
         .append(", id=").append(id)
         .append(", status=").append(status)
         .append(", value=").append(value)
         .append("}").toString
   }
   
}