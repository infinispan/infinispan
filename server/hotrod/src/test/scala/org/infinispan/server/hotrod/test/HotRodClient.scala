package org.infinispan.server.hotrod.test

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.testng.Assert._
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.Response
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.OperationResponse._
import org.infinispan.server.core.transport.NoState
import org.infinispan.server.core.transport.netty.{ChannelBufferAdapter}
import org.infinispan.server.core.Logging
import collection.mutable
import collection.immutable
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.concurrent.atomic.{AtomicLong}
import org.infinispan.test.TestingUtil
import org.infinispan.util.{ByteArrayKey, Util}

/**
 * A very simply Hot Rod client for testing purpouses
 *
 * Reasons why this should not really be a trait:
 * Storing var instances in a trait cause issues with TestNG, see:
 *   http://thread.gmane.org/gmane.comp.lang.scala.user/24317
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodClient(host: String, port: Int, defaultCacheName: String, rspTimeoutSeconds: Int) extends Logging {
   val idToOp = new ConcurrentHashMap[Long, Op]    

   private lazy val ch: Channel = {
      val factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)
      val bootstrap: ClientBootstrap = new ClientBootstrap(factory)
      bootstrap.setPipelineFactory(new ClientPipelineFactory(this, rspTimeoutSeconds))
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)
      // Make a new connection.
      val connectFuture = bootstrap.connect(new InetSocketAddress(host, port))
      // Wait until the connection is made successfully.
      val ch = connectFuture.awaitUninterruptibly.getChannel
      assertTrue(connectFuture.isSuccess)
      ch
   }
   
   def stop = ch.disconnect

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], clientIntelligence: Byte, topologyId: Int): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, clientIntelligence, topologyId)

   def assertPut(m: Method) {
      val status = put(k(m) , 0, 0, v(m)).status
      assertStatus(status, Success)
   }

   def assertPutFail(m: Method) {
      val op = new Op(0xA0, 0x01, defaultCacheName, k(m), 0, 0, v(m), 0, 1 , 0, 0)
      idToOp.put(op.id, op)
      val future = ch.write(op)
      future.awaitUninterruptibly
      assertFalse(future.isSuccess)
   }

   def assertPut(m: Method, kPrefix: String, vPrefix: String) {
      val status = put(k(m, kPrefix) , 0, 0, v(m, vPrefix)).status
      assertStatus(status, Success)
   }

   def assertPut(m: Method, lifespan: Int, maxIdle: Int) {
      val status = put(k(m) , lifespan, maxIdle, v(m)).status
      assertStatus(status, Success)
   }

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)
   
   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)   

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long): TestResponse =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, version, 1 ,0)

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long, flags: Int): TestResponse =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, version, flags)

   def remove(k: Array[Byte]): TestResponse =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0, 1 ,0)

   def remove(k: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0, flags)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long): TestResponse =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, version, 1 ,0)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long, flags: Int): TestResponse =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, version, flags)

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], version: Long, clientIntelligence: Byte, topologyId: Int): TestResponse = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, 0, version, clientIntelligence, topologyId)
      execute(op, op.id)
   }

   def executeExpectBadMagic(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
                           v: Array[Byte], version: Long): TestErrorResponse = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, 0, version, 1, 0)
      execute(op, 0).asInstanceOf[TestErrorResponse]
   }

   def executePartial(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
                      v: Array[Byte], version: Long): TestErrorResponse = {
      val op = new PartialOp(magic, code, name, k, lifespan, maxIdle, v, 0, version, 1, 0)
      execute(op, op.id).asInstanceOf[TestErrorResponse]
   }

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], version: Long, flags: Int): TestResponse = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, flags, version, 1, 0)
      execute(op, op.id)
   }

   private def execute(op: Op, expectedResponseMessageId: Long): TestResponse = {
      writeOp(op)
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(expectedResponseMessageId)
   }

   private def writeOp(op: Op) {
      idToOp.put(op.id, op)
      val future = ch.write(op)
      future.awaitUninterruptibly
      assertTrue(future.isSuccess)
   }

   def get(k: Array[Byte], flags: Int): TestGetResponse = {
      get(0x03, k, 0).asInstanceOf[TestGetResponse]
   }

   def assertGet(m: Method): TestGetResponse = assertGet(m, 0)

   def assertGet(m: Method, flags: Int): TestGetResponse = get(k(m), flags)

   def containsKey(k: Array[Byte], flags: Int): TestResponse = {
      get(0x0F, k, 0)
   }

   def getWithVersion(k: Array[Byte], flags: Int): TestGetWithVersionResponse =
      get(0x11, k, 0).asInstanceOf[TestGetWithVersionResponse]

   private def get(code: Byte, k: Array[Byte], flags: Int): TestResponse = {
      val op = new Op(0xA0, code, defaultCacheName, k, 0, 0, null, flags, 0, 1, 0)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      if (code == 0x03 || code == 0x11 || code == 0x0F) {
         handler.getResponse(op.id)
      } else {
         null
      }
   }

   def clear: TestResponse = execute(0xA0, 0x13, defaultCacheName, null, 0, 0, null, 0, 1 ,0)

   def stats: Map[String, String] = {
      val op = new StatsOp(0xA0, 0x15, defaultCacheName, 1, 0, null)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      val resp = handler.getResponse(op.id).asInstanceOf[TestStatsResponse]
      resp.stats
   }

   def ping: TestResponse = execute(0xA0, 0x17, defaultCacheName, null, 0, 0, null, 0, 1 ,0)

   def ping(clientIntelligence: Byte, topologyId: Int): TestResponse =
      execute(0xA0, 0x17, defaultCacheName, null, 0, 0, null, 0, clientIntelligence, topologyId)

   def bulkGet: TestBulkGetResponse = bulkGet(0)

   def bulkGet(count: Int): TestBulkGetResponse = {
      val op = new BulkGetOp(0xA0, 0x19, defaultCacheName, 1, 0, count)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetResponse]
   }
}

private class ClientPipelineFactory(client: HotRodClient, rspTimeoutSeconds: Int) extends ChannelPipelineFactory {

   override def getPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", new Decoder(client))
      pipeline.addLast("encoder", new Encoder)
      pipeline.addLast("handler", new ClientHandler(rspTimeoutSeconds))
      pipeline
   }

}

private class Encoder extends OneToOneEncoder {

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any) = {
      trace("Encode {0} so that it's sent to the server", msg)
      msg match {
         case partial: PartialOp => {
            val buffer = new ChannelBufferAdapter(ChannelBuffers.dynamicBuffer)
            buffer.writeByte(partial.magic.asInstanceOf[Byte]) // magic
            buffer.writeUnsignedLong(partial.id) // message id
            buffer.writeByte(10) // version
            buffer.writeByte(partial.code) // opcode
            buffer.getUnderlyingChannelBuffer
         }
         case op: Op => {
            val buffer = new ChannelBufferAdapter(ChannelBuffers.dynamicBuffer)
            buffer.writeByte(op.magic.asInstanceOf[Byte]) // magic
            buffer.writeUnsignedLong(op.id) // message id
            buffer.writeByte(10) // version
            buffer.writeByte(op.code) // opcode
            if (!op.cacheName.isEmpty) {
               buffer.writeRangedBytes(op.cacheName.getBytes()) // cache name length + cache name
            } else {
               buffer.writeUnsignedInt(0) // Zero length
            }
            buffer.writeUnsignedInt(op.flags) // flags
            buffer.writeByte(op.clientIntel) // client intelligence
            buffer.writeUnsignedInt(op.topologyId) // topology id
            buffer.writeRangedBytes(new Array[Byte](0))
            if (op.code != 0x13 && op.code != 0x15 && op.code != 0x17 && op.code != 0x19) { // if it's a key based op...
               buffer.writeRangedBytes(op.key) // key length + key
               if (op.value != null) {
                  if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                     buffer.writeUnsignedInt(op.lifespan) // lifespan
                     buffer.writeUnsignedInt(op.maxIdle) // maxIdle
                  }
                  if (op.code == 0x09 || op.code == 0x0D) {
                     buffer.writeLong(op.version)
                  }
                  if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                     buffer.writeRangedBytes(op.value) // value length + value
                  }
               }
            } else if (op.code == 0x19) {
               buffer.writeUnsignedInt(op.asInstanceOf[BulkGetOp].count) // Entry count
            }
            buffer.getUnderlyingChannelBuffer
         }
      }
   }

}

object HotRodClient {
   val idCounter = new AtomicLong
}

private class Decoder(client: HotRodClient) extends ReplayingDecoder[NoState] with Logging {

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: NoState): Object = {
      trace("Decode response from server")
      val buf = new ChannelBufferAdapter(buffer)
      val magic = buf.readUnsignedByte
      val id = buf.readUnsignedLong
      val opCode = OperationResponse.apply(buf.readUnsignedByte)
      val status = OperationStatus.apply(buf.readUnsignedByte)
      val topologyChangeMarker = buf.readUnsignedByte
      val op = client.idToOp.get(id)
      val topologyChangeResponse =
         if (topologyChangeMarker == 1) {
            val topologyId = buf.readUnsignedInt
            if (op.clientIntel == 2) {
               val numberClusterMembers = buf.readUnsignedInt
               val viewArray = new Array[TopologyAddress](numberClusterMembers)
               for (i <- 0 until numberClusterMembers) {
                  val host = buf.readString
                  val port = buf.readUnsignedShort
                  viewArray(i) = TopologyAddress(host, port, Map.empty, null)
               }
               Some(TopologyAwareResponse(TopologyView(topologyId, viewArray.toList)))
            } else if (op.clientIntel == 3) {
               val numOwners = buf.readUnsignedShort
               val hashFunction = buf.readByte
               val hashSpace = buf.readUnsignedInt
               val numberClusterMembers = buf.readUnsignedInt
               val viewArray = new Array[TopologyAddress](numberClusterMembers)
               for (i <- 0 until numberClusterMembers) {
                  val host = buf.readString
                  val port = buf.readUnsignedShort
                  val hashId = buf.readInt
                  viewArray(i) = TopologyAddress(host, port, Map(op.cacheName -> hashId), null)
               }
               Some(HashDistAwareResponse(TopologyView(topologyId, viewArray.toList), numOwners, hashFunction, hashSpace))
            } else {
               None // Is it possible?
            }
         } else {
            None
         }
      val resp: Response = opCode match {
         case StatsResponse => {
            val size = buf.readUnsignedInt
            val stats = mutable.Map.empty[String, String]
            for (i <- 1 to size) {
               stats += (buf.readString -> buf.readString)
            }
            new TestStatsResponse(id, op.cacheName, op.clientIntel, immutable.Map[String, String]() ++ stats, op.topologyId, topologyChangeResponse)
         }
         case PutResponse | PutIfAbsentResponse | ReplaceResponse | ReplaceIfUnmodifiedResponse
              | RemoveResponse | RemoveIfUnmodifiedResponse => {
            if (op.flags == 1) {
               val length = buf.readUnsignedInt
               if (length == 0) {
                  new TestResponseWithPrevious(id, op.cacheName, op.clientIntel, opCode, status,
                     op.topologyId, None, topologyChangeResponse)
               } else {
                  val previous = new Array[Byte](length)
                  buf.readBytes(previous)
                  new TestResponseWithPrevious(id, op.cacheName, op.clientIntel, opCode, status,
                     op.topologyId, Some(previous), topologyChangeResponse)
               }
            } else new TestResponse(id, op.cacheName, op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse)
         }
         case ContainsKeyResponse | ClearResponse | PingResponse =>
            new TestResponse(id, op.cacheName, op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse)
         case GetWithVersionResponse  => {
            if (status == Success) {
               val version = buf.readLong
               val data = Some(buf.readRangedBytes)
               new TestGetWithVersionResponse(id, op.cacheName, op.clientIntel, opCode, status,
                  op.topologyId, data, version, topologyChangeResponse)
            } else{
               new TestGetWithVersionResponse(id, op.cacheName, op.clientIntel, opCode, status,
                  op.topologyId, None, 0, topologyChangeResponse)
            }
         }
         case GetResponse => {
            if (status == Success) {
               val data = Some(buf.readRangedBytes)
               new TestGetResponse(id, op.cacheName, op.clientIntel, opCode, status, op.topologyId, data, topologyChangeResponse)
            } else{
               new TestGetResponse(id, op.cacheName, op.clientIntel, opCode, status, op.topologyId, None, topologyChangeResponse)
            }
         }
         case BulkGetResponse => {
            var done = buf.readByte
            val bulkBuffer = mutable.Map.empty[ByteArrayKey, Array[Byte]]
            while (done == 1) {
               bulkBuffer += (new ByteArrayKey(buf.readRangedBytes) -> buf.readRangedBytes)
               done = buf.readByte
            }
            val bulk = immutable.Map[ByteArrayKey, Array[Byte]]() ++ bulkBuffer
            new TestBulkGetResponse(id, op.cacheName, op.clientIntel, bulk, op.topologyId, topologyChangeResponse)
         }
         case ErrorResponse => {
            if (op == null)
               new TestErrorResponse(id, "", 0, status, 0, buf.readString, topologyChangeResponse)
            else
               new TestErrorResponse(id, op.cacheName, op.clientIntel, status, op.topologyId, buf.readString, topologyChangeResponse)
         }

      }
      trace("Got response from server: {0}", resp)
      resp
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }
}

private class ClientHandler(rspTimeoutSeconds: Int) extends SimpleChannelUpstreamHandler {

   private val responses = new ConcurrentHashMap[Long, TestResponse]

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val resp = e.getMessage.asInstanceOf[TestResponse]
      trace("Put {0} in responses", resp)
      responses.put(resp.messageId, resp)
   }

   def getResponse(messageId: Long): TestResponse = {
      // TODO: Very very primitive way of waiting for a response. Convert to a Future
      var i = 0;
      var v: TestResponse = null;
      do {
         v = responses.get(messageId)
         if (v == null) {
            TestingUtil.sleepThread(100)
            i += 1
         }
      }
      while (v == null && i < (rspTimeoutSeconds * 10))
      v
   }

}

class Op(val magic: Int,
         val code: Byte,
         val cacheName: String,
         val key: Array[Byte],
         val lifespan: Int,
         val maxIdle: Int,
         val value: Array[Byte],
         val flags: Int,
         val version: Long,
         val clientIntel: Byte,
         val topologyId: Int) {
   lazy val id = HotRodClient.idCounter.incrementAndGet
   override def toString = {
      new StringBuilder().append("Op").append("(")
         .append(id).append(',')
         .append(magic).append(',')
         .append(code).append(',')
         .append(cacheName).append(',')
         .append(if (key == null) "null" else Util.printArray(key, true)).append(',')
         .append(maxIdle).append(',')
         .append(lifespan).append(',')
         .append(if (value == null) "null" else Util.printArray(value, true)).append(',')
         .append(flags).append(',')
         .append(version).append(',')
         .append(clientIntel).append(',')
         .append(topologyId).append(')')
         .toString
   }

}

class PartialOp(override val magic: Int,
                override val code: Byte,
                override val cacheName: String,
                override val key: Array[Byte],
                override val lifespan: Int,
                override val maxIdle: Int,
                override val value: Array[Byte],
                override val flags: Int,
                override val version: Long,
                override val clientIntel: Byte,
                override val topologyId: Int)
      extends Op(magic, code, cacheName, key, lifespan, maxIdle, value, flags, version, clientIntel, topologyId) {
}

class StatsOp(override val magic: Int,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int,
              val statName: String) extends Op(magic, code, cacheName, null, 0, 0, null, 0, 0, clientIntel, topologyId)

class BulkGetOp(override val magic: Int,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int,
              val count: Int) extends Op(magic, code, cacheName, null, 0, 0, null, 0, 0, clientIntel, topologyId)

class TestResponse(override val messageId: Long, override val cacheName: String,
                   override val clientIntel: Short, override val operation: OperationResponse,
                   override val status: OperationStatus,
                   override val topologyId: Int,
                   val topologyResponse: Option[AbstractTopologyResponse])
      extends Response(messageId, cacheName, clientIntel, operation, status, topologyId)

class TestResponseWithPrevious(override val messageId: Long, override val cacheName: String,
                           override val clientIntel: Short, override val operation: OperationResponse,
                           override val status: OperationStatus,
                           override val topologyId: Int, val previous: Option[Array[Byte]],
                           override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestResponse(messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse)

class TestGetResponse(override val messageId: Long, override val cacheName: String, override val clientIntel: Short,
                  override val operation: OperationResponse, override val status: OperationStatus,
                  override val topologyId: Int, val data: Option[Array[Byte]],
                  override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestResponse(messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse)

class TestGetWithVersionResponse(override val messageId: Long, override val cacheName: String,
                             override val clientIntel: Short, override val operation: OperationResponse,
                             override val status: OperationStatus,
                             override val topologyId: Int,
                             override val data: Option[Array[Byte]], val version: Long,
                             override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestGetResponse(messageId, cacheName, clientIntel, operation, status, topologyId, data, topologyResponse)

class TestErrorResponse(override val messageId: Long, override val cacheName: String,
                    override val clientIntel: Short, override val status: OperationStatus,
                    override val topologyId: Int, val msg: String,
                    override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestResponse(messageId, cacheName, clientIntel, ErrorResponse, status, topologyId, topologyResponse)

class TestStatsResponse(override val messageId: Long, override val cacheName: String,
                        override val clientIntel: Short, val stats: Map[String, String],
                        override val topologyId: Int, override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestResponse(messageId, cacheName, clientIntel, StatsResponse, Success, topologyId, topologyResponse)

class TestBulkGetResponse(override val messageId: Long, override val cacheName: String,
                          override val clientIntel: Short, val bulkData: Map[ByteArrayKey, Array[Byte]],
                          override val topologyId: Int, override val topologyResponse: Option[AbstractTopologyResponse])
      extends TestResponse(messageId, cacheName, clientIntel, BulkGetResponse, Success, topologyId, topologyResponse)