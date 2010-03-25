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
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.server.core.transport.NoState
import org.jboss.netty.channel.ChannelHandler.Sharable
import java.util.Arrays
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, Executors}
import org.infinispan.server.core.transport.netty.{ChannelBufferAdapter}
import org.infinispan.server.core.Logging

/**
 * // TODO: Document this
 *
 * // TODO: Transform to Netty independent code
 * // TODO: maybe make it an object to be able to cache stuff without testng complaining
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Client {

   def connect(host: String, port: Int): Channel = {
      // Set up.
      val factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)
      val bootstrap: ClientBootstrap = new ClientBootstrap(factory)
      bootstrap.setPipelineFactory(ClientPipelineFactory)
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)
      // Make a new connection.
      val connectFuture = bootstrap.connect(new InetSocketAddress(host, port))
      // Wait until the connection is made successfully.
      val ch = connectFuture.awaitUninterruptibly.getChannel
      // Ideally, I'd store channel as a var in this trait. However, this causes issues with TestNG, see:
      // http://thread.gmane.org/gmane.comp.lang.scala.user/24317
      assertTrue(connectFuture.isSuccess)
      ch
   }

   def put(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus = {
      put(ch, 0xA0, 0x01, name, k, lifespan, maxIdle, v, 0, 0)
   }

   def put(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
           flags: Int): OperationStatus = {
      put(ch, 0xA0, 0x01, name, k, lifespan, maxIdle, v, flags, 0)
   }

   def putIfAbsent(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus = {
      put(ch, 0xA0, 0x05, name, k, lifespan, maxIdle, v, 0, 0)
   }

   def replace(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): OperationStatus = {
      put(ch, 0xA0, 0x07, name, k, lifespan, maxIdle, v, 0, 0)
   }

   def replaceIfUnmodified(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                           version: Long): OperationStatus = {
      put(ch, 0xA0, 0x09, name, k, lifespan, maxIdle, v, 0, version)
   }

   def removeIfUnmodified(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                           version: Long): OperationStatus = {
      put(ch, 0xA0, 0x0D, name, k, lifespan, maxIdle, v, 0, version)
   }

   // TODO: change name of this method since it's more polivalent than just a put
   def put(ch: Channel, magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
           v: Array[Byte], flags: Int, version: Long): OperationStatus = {
      val writeFuture = ch.write(new Op(magic, code, name, k, lifespan, maxIdle, v, flags, version))
      writeFuture.awaitUninterruptibly
      assertTrue(writeFuture.isSuccess)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse.status
   }

   def get(ch: Channel, name: String, k: Array[Byte], flags: Int): (OperationStatus, Array[Byte]) = {
      val (getSt, actual, version) = get(ch, 0x03, name, k, 0)
      (getSt, actual)
   }

   def containsKey(ch: Channel, name: String, k: Array[Byte], flags: Int): OperationStatus = {
      val (containsKeySt, actual, version) = get(ch, 0x0F, name, k, 0)
      containsKeySt
   }

   def getWithVersion(ch: Channel, name: String, k: Array[Byte], flags: Int): (OperationStatus, Array[Byte], Long) = {
      get(ch, 0x11, name, k, 0)
   }

   def remove(ch: Channel, name: String, k: Array[Byte], flags: Int): OperationStatus = {
      put(ch, 0xA0, 0x0B, name, k, 0, 0, null, 0, 0)
   }

   def get(ch: Channel, code: Byte, name: String, k: Array[Byte], flags: Int): (OperationStatus, Array[Byte], Long) = {
      val writeFuture = ch.write(new Op(0xA0, code, name, k, 0, 0, null, flags, 0))
      writeFuture.awaitUninterruptibly
      assertTrue(writeFuture.isSuccess)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      if (code == 0x03) {
         val resp = handler.getResponse.asInstanceOf[GetResponse]
         (resp.status, if (resp.data == None) null else resp.data.get, 0)
      } else if (code == 0x11) {
         val resp = handler.getResponse.asInstanceOf[GetWithVersionResponse]
         (resp.status, if (resp.data == None) null else resp.data.get, resp.version)
      } else if (code == 0x0F) {
         (handler.getResponse.status, null, 0)
      } else {
         (OperationNotExecuted, null, 0)
      }
   }

   def clear(ch: Channel, name: String): OperationStatus = {
      put(ch, 0xA0, 0x13, name, null, 0, 0, null, 0, 0)
//      val writeFuture = ch.write(new Op(0xA0, , name, null, 0, 0, null, 0, 0))
//      writeFuture.awaitUninterruptibly
//      assertTrue(writeFuture.isSuccess)
//      // Get the handler instance to retrieve the answer.
//      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
//      handler.getResponse.status
   }

   def ping(ch: Channel, name: String): OperationStatus = {
      put(ch, 0xA0, 0x17, name, null, 0, 0, null, 0, 0)
//      val writeFuture = ch.write(new Op(0xA0, 0x13, name, null, 0, 0, null, 0, 0))
//      writeFuture.awaitUninterruptibly
//      assertTrue(writeFuture.isSuccess)
//      // Get the handler instance to retrieve the answer.
//      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
//      handler.getResponse.status
   }


   def assertStatus(status: OperationStatus, expected: OperationStatus): Boolean = {
      val isSuccess = status == expected
      assertTrue(isSuccess, "Status should have been '" + expected + "' but instead was: " + status)
      isSuccess
   }

   def assertSuccess(status: OperationStatus, expected: Array[Byte], actual: Array[Byte]): Boolean = {
      assertStatus(status, Success)
      val isSuccess = Arrays.equals(expected, actual)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertKeyDoesNotExist(status: OperationStatus, actual: Array[Byte]): Boolean = {
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertNull(actual)
      status == KeyDoesNotExist
   }

}

@Sharable
private object ClientPipelineFactory extends ChannelPipelineFactory {

   override def getPipeline() = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", Decoder)
      pipeline.addLast("encoder", Encoder)
      pipeline.addLast("handler", new ClientHandler)
      pipeline
   }

}

@Sharable
private object Encoder extends OneToOneEncoder {

   private val idCounter: AtomicInteger = new AtomicInteger

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any) = {
      val ret =
         msg match {
            case op: Op => {
               val buffer = new ChannelBufferAdapter(ChannelBuffers.dynamicBuffer)
               buffer.writeByte(op.magic.asInstanceOf[Byte]) // magic
               buffer.writeUnsignedLong(idCounter.incrementAndGet) // message id
               buffer.writeByte(10) // version
               buffer.writeByte(op.code) // opcode
               buffer.writeRangedBytes(op.cacheName.getBytes()) // cache name length + cache name
               buffer.writeUnsignedInt(op.flags) // flags
               buffer.writeByte(0) // client intelligence
               buffer.writeUnsignedInt(0) // topology id
               if (op.code != 0x13 && op.code != 0x17) { // if it's a key based op... 
                  buffer.writeRangedBytes(op.key) // key length + key
                  if (op.value != null) {
                     buffer.writeUnsignedInt(op.lifespan) // lifespan
                     buffer.writeUnsignedInt(op.maxIdle) // maxIdle
                     if (op.code == 0x09 || op.code == 0x0D) {
                        buffer.writeLong(op.version)
                     }
                     buffer.writeRangedBytes(op.value) // value length + value
                  }
               }
               buffer.getUnderlyingChannelBuffer
            }
      }
      ret
   }

}

private object Decoder extends ReplayingDecoder[NoState] with Logging {

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: NoState): Object = {
      val buf = new ChannelBufferAdapter(buffer)
      val magic = buf.readUnsignedByte
      val id = buf.readUnsignedLong
      // val opCode = OperationResolver.resolve(buf.readUnsignedByte)
      val opCode = OperationResponse.apply(buf.readUnsignedByte)
      val status = OperationStatus.apply(buf.readUnsignedByte)
      val topologyChangeMarker = buf.readUnsignedByte
      val resp: Response =
         opCode match {
//            case StatsResponse => {
//               // TODO!!! Wait for outcome of mail
//            }
            case PutResponse | PutIfAbsentResponse | ReplaceResponse | ReplaceIfUnmodifiedResponse
                 | RemoveResponse | RemoveIfUnmodifiedResponse | ContainsKeyResponse | ClearResponse | PingResponse =>
               new Response(id, opCode, status)
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
      resp
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }
}

private class ClientHandler extends SimpleChannelUpstreamHandler {

   private val answer = new LinkedBlockingQueue[Response];

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val offered = answer.offer(e.getMessage.asInstanceOf[Response])
      assertTrue(offered)
   }

   def getResponse: Response = {
      answer.poll(60, TimeUnit.SECONDS)
   }

}

private class Op(val magic: Int,
                 val code: Byte,
                 val cacheName: String,
                 val key: Array[Byte],
                 val lifespan: Int,
                 val maxIdle: Int,
                 val value: Array[Byte],
                 val flags: Int,
                 val version: Long)