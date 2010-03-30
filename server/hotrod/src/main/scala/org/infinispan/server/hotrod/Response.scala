package org.infinispan.server.hotrod

import OperationStatus._
import OperationResponse._
import org.infinispan.util.Util

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class Response(val messageId: Long, val operation: OperationResponse, val status: OperationStatus) {
   override def toString = {
      new StringBuilder().append("Response").append("{")
         .append("messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append("}").toString
   }
}

class ResponseWithPrevious(override val messageId: Long, override val operation: OperationResponse,
                                override val status: OperationStatus, val previous: Option[Array[Byte]])
      extends Response(messageId, operation, status) {
   override def toString = {
      new StringBuilder().append("ResponseWithPrevious").append("{")
         .append("messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", previous=").append(if (previous == None) "null" else Util.printArray(previous.get, true))
         .append("}").toString
   }
}

class GetResponse(override val messageId: Long, override val operation: OperationResponse,
                  override val status: OperationStatus, val data: Option[Array[Byte]])
      extends Response(messageId, operation, status) {
   override def toString = {
      new StringBuilder().append("GetResponse").append("{")
         .append("messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append(if (data == None) "null" else Util.printArray(data.get, true))
         .append("}").toString
   }
}

class GetWithVersionResponse(override val messageId: Long, override val operation: OperationResponse,
                             override val status: OperationStatus, override val data: Option[Array[Byte]],
                             val version: Long)
      extends GetResponse(messageId, operation, status, data) {
   override def toString = {
      new StringBuilder().append("GetWithVersionResponse").append("{")
         .append("messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append(if (data == None) "null" else Util.printArray(data.get, true))
         .append(", version=").append(version)
         .append("}").toString
   }
}

class ErrorResponse(override val messageId: Long, override val status: OperationStatus,
                    val msg: String) extends Response(messageId, ErrorResponse, status) {
   override def toString = {
      new StringBuilder().append("ErrorResponse").append("{")
         .append("messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", msg=").append(msg)
         .append("}").toString
   }
}

class StatsResponse(override val messageId: Long, val stats: Map[String, String]) extends Response(messageId, StatsResponse, Success) {
   override def toString = {
      new StringBuilder().append("StatsResponse").append("{")
         .append("messageId=").append(messageId)
         .append(", stats=").append(stats)
         .append("}").toString
   }
}