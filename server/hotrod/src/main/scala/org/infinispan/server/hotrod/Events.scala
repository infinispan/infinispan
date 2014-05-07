package org.infinispan.server.hotrod

import org.infinispan.commons.util.Util
import org.infinispan.server.hotrod.OperationResponse._

/**
 * @author Galder Zamarreño
 */
object Events {

   abstract class Event(val version: Byte, val messageId: Long, val op: OperationResponse, val listenerId: Bytes)

   case class KeyEvent(
           override val version: Byte,
           override val messageId: Long,
           override val listenerId: Bytes,
           key: Bytes)
         extends Event(version, messageId, CacheEntryRemovedEventResponse, listenerId) {
      override def toString: String = {
         new StringBuilder().append("KeyEvent").append("{")
                 .append("version=").append(version)
                 .append(", messageId=").append(messageId)
                 .append(", listenerId=").append(Util.printArray(listenerId, false))
                 .append(", key=").append(Util.printArray(key, false))
                 .append("}").toString()
      }
   }

   case class KeyWithVersionEvent(
           override val version: Byte,
           override val messageId: Long,
           override val op: OperationResponse,
           override val listenerId: Bytes,
           key: Bytes, dataVersion: Long)
         extends Event(version, messageId, op, listenerId) {
      new StringBuilder().append("KeyWithVersionEvent").append("{")
              .append("version=").append(version)
              .append(", messageId=").append(messageId)
              .append(", listenerId=").append(Util.printArray(listenerId))
              .append(", key=").append(Util.printArray(key))
              .append(", dataVersion=").append(dataVersion)
              .append("}").toString()
   }

   case class CustomEvent(
               override val version: Byte,
               override val messageId: Long,
               override val op: OperationResponse,
               override val listenerId: Bytes,
               eventData: Bytes)
           extends Event(version, messageId, op, listenerId) {
      new StringBuilder().append("CustomEvent").append("{")
              .append("version=").append(version)
              .append(", messageId=").append(messageId)
              .append(", listenerId=").append(Util.printArray(listenerId, false))
              .append(", event=").append(Util.printArray(eventData, false))
              .append("}").toString()
   }

}
