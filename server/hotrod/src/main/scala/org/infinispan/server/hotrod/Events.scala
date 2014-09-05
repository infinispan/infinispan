package org.infinispan.server.hotrod

import org.infinispan.commons.util.Util
import org.infinispan.server.hotrod.OperationResponse._

/**
 * @author Galder Zamarreño
 */
object Events {

   abstract class Event(val version: Byte, val messageId: Long, val op: OperationResponse, val listenerId: Bytes, val isRetried: Boolean)

   case class KeyEvent(
           override val version: Byte,
           override val messageId: Long,
           override val listenerId: Bytes,
           override val isRetried: Boolean,
           key: Bytes)
         extends Event(version, messageId, CacheEntryRemovedEventResponse, listenerId, isRetried) {
      override def toString: String = {
         new StringBuilder().append("KeyEvent").append("{")
                 .append("version=").append(version)
                 .append(", messageId=").append(messageId)
                 .append(", listenerId=").append(Util.printArray(listenerId, false))
                 .append(", key=").append(Util.toStr(key))
                 .append(", isRetried=").append(isRetried)
                 .append("}").toString()
      }
   }

   case class KeyWithVersionEvent(
           override val version: Byte,
           override val messageId: Long,
           override val op: OperationResponse,
           override val listenerId: Bytes,
           override val isRetried: Boolean,
           key: Bytes, dataVersion: Long)
         extends Event(version, messageId, op, listenerId, isRetried) {
      override def toString: String = {
         new StringBuilder().append("KeyWithVersionEvent").append("{")
                 .append("version=").append(version)
                 .append(", messageId=").append(messageId)
                 .append(", op=").append(op)
                 .append(", listenerId=").append(Util.toStr(listenerId))
                 .append(", key=").append(Util.toStr(key))
                 .append(", dataVersion=").append(dataVersion)
                 .append(", isRetried=").append(isRetried)
                 .append("}").toString()
      }
   }

   case class CustomEvent(
               override val version: Byte,
               override val messageId: Long,
               override val op: OperationResponse,
               override val listenerId: Bytes,
               override val isRetried: Boolean,
               eventData: Bytes)
           extends Event(version, messageId, op, listenerId, isRetried) {
      override def toString: String = {
         new StringBuilder().append("CustomEvent").append("{")
                 .append("version=").append(version)
                 .append(", messageId=").append(messageId)
                 .append(", op=").append(op)
                 .append(", listenerId=").append(Util.toStr(listenerId, false))
                 .append(", event=").append(Util.toStr(eventData, false))
                 .append(", isRetried=").append(isRetried)
                 .append("}").toString()
      }
   }

}
