package org.infinispan.server.hotrod

import OperationStatus._
import OperationResponse._
import org.infinispan.commons.util.Util
import org.infinispan.remoting.transport.Address
import java.lang.StringBuilder

/**
 * A basic responses. The rest of this file contains other response types.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class Response(val version: Byte, val messageId: Long, val cacheName: String,
      val clientIntel: Short, val operation: OperationResponse,
      val status: OperationStatus, val topologyId: Int) {
   override def toString = {
      new StringBuilder().append("Response").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", cacheName=").append(cacheName)
         .append("}").toString()
   }
}

class ResponseWithPrevious(override val version: Byte, override val messageId: Long,
                           override val cacheName: String, override val clientIntel: Short,
                           override val operation: OperationResponse,
                           override val status: OperationStatus,
                           override val topologyId: Int,
                           val previous: Option[Array[Byte]])
      extends Response(version, messageId, cacheName, clientIntel, operation, status, topologyId) {
   override def toString = {
      new StringBuilder().append("ResponseWithPrevious").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", previous=").append(Util.printArray(previous.getOrElse(null), true))
         .append("}").toString
   }
}

class GetResponse(override val version: Byte, override val messageId: Long, override val cacheName: String, override val clientIntel: Short,
                  override val operation: OperationResponse, override val status: OperationStatus,
                  override val topologyId: Int, val data: Option[Array[Byte]])
      extends Response(version, messageId, cacheName, clientIntel, operation, status, topologyId) {
   override def toString = {
      new StringBuilder().append("GetResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append(Util.printArray(data.getOrElse(null), true))
         .append("}").toString
   }
}
class BulkGetResponse(override val version: Byte, override val messageId: Long, override val cacheName: String, override val clientIntel: Short,
                  override val operation: OperationResponse, override val status: OperationStatus,
                  override val topologyId: Int, val count: Int)
      extends Response(version, messageId, cacheName, clientIntel, operation, status, topologyId) {
   override def toString = {
      new StringBuilder().append("BulkGetResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append("}").toString
   }
}

class BulkGetKeysResponse(override val version: Byte, override val messageId: Long, override val cacheName: String, override val clientIntel: Short,
                  override val operation: OperationResponse, override val status: OperationStatus,
                  override val topologyId: Int, val scope: Int)
      extends Response(version, messageId, cacheName, clientIntel, operation, status, topologyId) {
   override def toString = {
      new StringBuilder().append("BulkGetKeysResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=")
         .append(", scope=").append(scope)
         .append("}").toString
   }
}

class GetWithVersionResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                             override val clientIntel: Short, override val operation: OperationResponse,
                             override val status: OperationStatus,
                             override val topologyId: Int,
                             override val data: Option[Array[Byte]], val dataVersion: Long)
      extends GetResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, data) {
   override def toString = {
      new StringBuilder().append("GetWithVersionResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append(Util.printArray(data.getOrElse(null), true))
         .append(", dataVersion=").append(dataVersion)
         .append("}").toString
   }
}

class GetWithMetadataResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                             override val clientIntel: Short, override val operation: OperationResponse,
                             override val status: OperationStatus,
                             override val topologyId: Int,
                             override val data: Option[Array[Byte]], val dataVersion: Long, val created: Long, val lifespan: Int, val lastUsed: Long, val maxIdle: Int)
      extends GetResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, data) {
   override def toString = {
      new StringBuilder().append("GetWithMetadataResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", data=").append(Util.printArray(data.getOrElse(null), true))
         .append(", dataVersion=").append(dataVersion)
         .append(", created=").append(created)
         .append(", lifespan=").append(lifespan)
         .append(", lastUsed=").append(lastUsed)
         .append(", maxIdle=").append(maxIdle)
         .append("}").toString
   }
}

class ErrorResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                    override val clientIntel: Short, override val status: OperationStatus,
                    override val topologyId: Int, val msg: String)
      extends Response(version, messageId, cacheName, clientIntel, ErrorResponse, status, topologyId) {
   override def toString = {
      new StringBuilder().append("ErrorResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", msg=").append(msg)
         .append("}").toString
   }
}

class StatsResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                    override val clientIntel: Short, val stats: Map[String, String],
                    override val topologyId: Int)
      extends Response(version, messageId, cacheName, clientIntel, StatsResponse, Success, topologyId) {
   override def toString = {
      new StringBuilder().append("StatsResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", stats=").append(stats)
         .append("}").toString
   }
}

class QueryResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
        override val clientIntel: Short, override val topologyId: Int, val result: Array[Byte])
      extends Response(version, messageId, cacheName, clientIntel, QueryResponse, Success, topologyId) {
   override def toString: String = {
      new StringBuilder().append("QueryResponse").append("{")
              .append("version=").append(version)
              .append(", messageId=").append(messageId)
              .append(", result=").append(Util.printArray(result, true))
              .append("}").toString
   }
}

class AuthMechListResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                    override val clientIntel: Short, val mechs: Set[String],
                    override val topologyId: Int)
      extends Response(version, messageId, cacheName, clientIntel, AuthMechListResponse, Success, topologyId) {
   override def toString = {
      new StringBuilder().append("AuthMechListResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", mechs=").append(mechs)
         .append("}").toString
   }
}

class AuthResponse(override val version: Byte, override val messageId: Long, override val cacheName: String,
                    override val clientIntel: Short, val challenge: Array[Byte], override val topologyId: Int)
      extends Response(version, messageId, cacheName, clientIntel, AuthResponse, Success, topologyId) {
   override def toString = {
      new StringBuilder().append("AuthResponse").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", challenge=").append(Util.printArray(challenge, true))
         .append("}").toString
   }
}

abstract class AbstractTopologyResponse(val topologyId: Int, val serverEndpointsMap : Map[Address, ServerAddress])

abstract class AbstractHashDistAwareResponse(override val topologyId: Int,
                                             override val serverEndpointsMap : Map[Address, ServerAddress],
                                             val numOwners: Int, val hashFunction: Byte, val hashSpace: Int)
        extends AbstractTopologyResponse(topologyId, serverEndpointsMap)

case class TopologyAwareResponse(override val topologyId: Int,
                                 override val serverEndpointsMap : Map[Address, ServerAddress])
      extends AbstractTopologyResponse(topologyId, serverEndpointsMap)

case class HashDistAwareResponse(override val topologyId: Int,
                                 override val serverEndpointsMap : Map[Address, ServerAddress],
                                 override val numOwners: Int, override val hashFunction: Byte,
                                 override val hashSpace: Int)
        extends AbstractHashDistAwareResponse(topologyId, serverEndpointsMap, numOwners, hashFunction, hashSpace)

case class HashDistAware11Response(override val topologyId: Int,
                                   override val serverEndpointsMap : Map[Address, ServerAddress],
                                   override val numOwners: Int, override val hashFunction: Byte,
                                   override val hashSpace: Int, numVNodes: Int)
        extends AbstractHashDistAwareResponse(topologyId, serverEndpointsMap, numOwners, hashFunction, hashSpace)

case class HashDistAware20Response(override val topologyId: Int,
        override val serverEndpointsMap : Map[Address, ServerAddress],
        hashFunction: Byte)
        extends AbstractTopologyResponse(topologyId, serverEndpointsMap)
