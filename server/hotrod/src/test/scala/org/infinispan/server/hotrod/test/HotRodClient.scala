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
class HotRodClient(host: String, port: Int, defaultCacheName: String) {
   val idToOp = new ConcurrentHashMap[Long, Op]    

   private lazy val ch: Channel = {
      val factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)
      val bootstrap: ClientBootstrap = new ClientBootstrap(factory)
      bootstrap.setPipelineFactory(new ClientPipelineFactory(this))
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

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0)

   def assertPut(m: Method) {
      val status = put(k(m) , 0, 0, v(m))
      assertStatus(status, Success)
   }

   def assertPut(m: Method, kPrefix: String, vPrefix: String) {
      val status = put(k(m, kPrefix) , 0, 0, v(m, vPrefix))
      assertStatus(status, Success)
   }

   def assertPut(m: Method, lifespan: Int, maxIdle: Int) {
      val status = put(k(m) , lifespan, maxIdle, v(m))
      assertStatus(status, Success)
   }

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)
   
   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0)

   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)   

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long): OperationStatus =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, version)

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long, flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, version, flags)

   def remove(k: Array[Byte]): OperationStatus =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0)

   def remove(k: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0, flags)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long): OperationStatus =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, version)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], version: Long, flags: Int): (OperationStatus, Array[Byte]) =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, version, flags)

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], version: Long): OperationStatus = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, 0, version)
      execute(op, op.id)._1
   }

   def executeWithBadMagic(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
                           v: Array[Byte], version: Long): OperationStatus = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, 0, version)
      execute(op, 0)._1
   }

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], version: Long, flags: Int): (OperationStatus, Array[Byte]) = {
      val op = new Op(magic, code, name, k, lifespan, maxIdle, v, flags, version)
      execute(op, op.id)
   }

   private def execute(op: Op, expectedResponseMessageId: Long): (OperationStatus, Array[Byte]) = {
      writeOp(op)
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      if (op.flags == 1) {
         val respWithPrevious = handler.getResponse(expectedResponseMessageId).asInstanceOf[ResponseWithPrevious]
         if (respWithPrevious.previous == None)
            (respWithPrevious.status, Array())
         else
            (respWithPrevious.status, respWithPrevious.previous.get)
      } else {
         (handler.getResponse(expectedResponseMessageId).status, null)
      }
   }

   private def writeOp(op: Op) {
      idToOp.put(op.id, op)
      val future = ch.write(op)
      future.awaitUninterruptibly
      assertTrue(future.isSuccess)
   }

   def get(k: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) = {
      val (getSt, actual, version) = get(0x03, k, 0)
      (getSt, actual)
   }

   def assertGet(m: Method): (OperationStatus, Array[Byte]) = assertGet(m, 0)

   def assertGet(m: Method, flags: Int): (OperationStatus, Array[Byte]) = get(k(m), flags)   

   def containsKey(k: Array[Byte], flags: Int): OperationStatus = {
      val (containsKeySt, actual, version) = get(0x0F, k, 0)
      containsKeySt
   }

   def getWithVersion(k: Array[Byte], flags: Int): (OperationStatus, Array[Byte], Long) =
      get(0x11, k, 0)

   private def get(code: Byte, k: Array[Byte], flags: Int): (OperationStatus, Array[Byte], Long) = {
      val op = new Op(0xA0, code, defaultCacheName, k, 0, 0, null, flags, 0)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      if (code == 0x03) {
         val resp = handler.getResponse(op.id).asInstanceOf[GetResponse]
         (resp.status, if (resp.data == None) null else resp.data.get, 0)
      } else if (code == 0x11) {
         val resp = handler.getResponse(op.id).asInstanceOf[GetWithVersionResponse]
         (resp.status, if (resp.data == None) null else resp.data.get, resp.version)
      } else if (code == 0x0F) {
         (handler.getResponse(op.id).status, null, 0)
      } else {
         (OperationNotExecuted, null, 0)
      }
   }

   def clear: OperationStatus = execute(0xA0, 0x13, defaultCacheName, null, 0, 0, null, 0)

   def stats: Map[String, String] = {
      val op = new StatsOp(0xA0, 0x15, defaultCacheName, null)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      val resp = handler.getResponse(op.id).asInstanceOf[StatsResponse]
      resp.stats
   }

   def ping: OperationStatus = execute(0xA0, 0x17, defaultCacheName, null, 0, 0, null, 0)

}

private class ClientPipelineFactory(client: HotRodClient) extends ChannelPipelineFactory {

   override def getPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", new Decoder(client))
      pipeline.addLast("encoder", new Encoder)
      pipeline.addLast("handler", new ClientHandler)
      pipeline
   }

}

private class Encoder extends OneToOneEncoder {

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any) = {
      trace("Encode {0} so that it's sent to the server", msg)
      msg match {
         case op: Op => {
            val buffer = new ChannelBufferAdapter(ChannelBuffers.dynamicBuffer)
            buffer.writeByte(op.magic.asInstanceOf[Byte]) // magic
            buffer.writeUnsignedLong(op.id) // message id
            buffer.writeByte(10) // version
            buffer.writeByte(op.code) // opcode
            buffer.writeRangedBytes(op.cacheName.getBytes()) // cache name length + cache name
            buffer.writeUnsignedInt(op.flags) // flags
            buffer.writeByte(0) // client intelligence
            buffer.writeUnsignedInt(0) // topology id
            if (op.code != 0x13 && op.code != 0x15 && op.code != 0x17) { // if it's a key based op...
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
      val resp: Response = opCode match {
         case StatsResponse => {
            val size = buf.readUnsignedInt
            val stats = mutable.Map.empty[String, String]
            for (i <- 1 to size) {
               stats += (buf.readString -> buf.readString)
            }
            new StatsResponse(id, immutable.Map[String, String]() ++ stats)
         }
         case PutResponse | PutIfAbsentResponse | ReplaceResponse | ReplaceIfUnmodifiedResponse
              | RemoveResponse | RemoveIfUnmodifiedResponse => {
            val op = client.idToOp.get(id)
            if (op.flags == 1) {
               val length = buf.readUnsignedInt
               if (length == 0) {
                  new ResponseWithPrevious(id, opCode, status, None)
               } else {
                  val previous = new Array[Byte](length)
                  buf.readBytes(previous)
                  new ResponseWithPrevious(id, opCode, status, Some(previous))
               }
            } else new Response(id, opCode, status)
         }
         case ContainsKeyResponse | ClearResponse | PingResponse => new Response(id, opCode, status)
         case GetWithVersionResponse  => {
            if (status == Success) {
               val version = buf.readLong
               val data = Some(buf.readRangedBytes)
               new GetWithVersionResponse(id, opCode, status, data, version)
            } else{
               new GetWithVersionResponse(id, opCode, status, None, 0)
            }
         }
         case GetResponse => {
            if (status == Success) {
               val data = Some(buf.readRangedBytes)
               new GetResponse(id, opCode, status, data)
            } else{
               new GetResponse(id, opCode, status, None)
            }
         }
         case ErrorResponse => new ErrorResponse(id, status, buf.readString)
      }
      trace("Got response from server: {0}", resp)
      resp
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }
}

private class ClientHandler extends SimpleChannelUpstreamHandler {

   private val responses = new ConcurrentHashMap[Long, Response]

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val resp = e.getMessage.asInstanceOf[Response]
      trace("Put {0} in responses", resp)
      responses.put(resp.messageId, resp)
   }

   def getResponse(messageId: Long): Response = {
      // TODO: Very very primitive way of waiting for a response. Convert to a Future
      var i = 0;
      var v: Response = null;
      do {
         v = responses.get(messageId)
         if (v == null) {
            TestingUtil.sleepThread(100)
            i += 1
         }
      }
      while (v == null && i < 100)
      v
   }

}

case class Op(val magic: Int,
                 val code: Byte,
                 val cacheName: String,
                 val key: Array[Byte],
                 val lifespan: Int,
                 val maxIdle: Int,
                 val value: Array[Byte],
                 val flags: Int,
                 val version: Long) {
   lazy val id = HotRodClient.idCounter.incrementAndGet
}

class StatsOp(override val magic: Int,
                 override val code: Byte,
                 override val cacheName: String,
                 val statName: String) extends Op(magic, code, cacheName, null, 0, 0, null, 0, 0)