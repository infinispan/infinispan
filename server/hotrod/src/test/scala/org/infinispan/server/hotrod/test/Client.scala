package org.infinispan.server.hotrod.test

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.infinispan.server.core.transport.netty.NettyChannelBuffer
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.testng.Assert._
import java.util.concurrent.{LinkedBlockingQueue, Executors}
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.OpCodes._
import org.infinispan.server.hotrod.Status._
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.server.core.transport.NoState
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.infinispan.context.Flag
import java.util.Arrays

/**
 * // TODO: Document this
 *
 * // TODO: Transform to Netty independent code
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

   def put(ch: Channel, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): Status = {
      val writeFuture = ch.write(new Op(0x01, name, k, lifespan, maxIdle, v, null))
      writeFuture.awaitUninterruptibly
      assertTrue(writeFuture.isSuccess)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse.status
   }

//   def get(ch: Channel, name: String, key: Array[Byte]): (Status.Status, Array[Byte]) = {
//      get(ch, name, key, null)
//   }

   def get(ch: Channel, name: String, k: Array[Byte], flags: Set[Flag]): (Status.Status, Array[Byte]) = {
      val writeFuture = ch.write(new Op(0x03, name, k, 0, 0, null, flags))
      writeFuture.awaitUninterruptibly
      assertTrue(writeFuture.isSuccess)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      val resp = handler.getResponse.asInstanceOf[RetrievalResponse]
      (resp.status, resp.value)
   }

   def assertSuccess(status: Status.Status): Boolean = {
      val isSuccess = status == Success
      assertTrue(isSuccess, "Status should have been 'Success' but instead was: " + status)
      isSuccess
   }

   def assertSuccess(status: Status.Status, expected: Array[Byte], actual: Array[Byte]): Boolean = {
      assertSuccess(status)
      val isSuccess = Arrays.equals(expected, actual)
      assertTrue(isSuccess)
      isSuccess
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
               val buffer = new NettyChannelBuffer(ChannelBuffers.dynamicBuffer)
               buffer.writeByte(0xA0.asInstanceOf[Byte]) // magic
               buffer.writeByte(41) // version
               buffer.writeByte(op.code) // opcode
               buffer.writeRangedBytes(op.cacheName.getBytes()) // cache name length + cache name
               buffer.writeUnsignedLong(idCounter.incrementAndGet) // message id
               if (op.flags != null)
                  buffer.writeUnsignedInt(Flags.fromContextFlags(op.flags)) // flags
               else
                  buffer.writeUnsignedInt(0) // flags

               buffer.writeRangedBytes(op.key) // key length + key
               if (op.value != null) {
                  buffer.writeUnsignedInt(op.lifespan) // lifespan
                  buffer.writeUnsignedInt(op.maxIdle) // maxIdle
                  buffer.writeRangedBytes(op.value) // value length + value
               }
               buffer.getUnderlyingChannelBuffer
            }
      }
      ret
   }

}

private object Decoder extends ReplayingDecoder[NoState] with Logging {

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: NoState): Object = {
      val buf = new NettyChannelBuffer(buffer)
      val magic = buf.readUnsignedByte
      val opCode = OpCodes.apply(buf.readUnsignedByte)
      val id = buf.readUnsignedLong
      val status = Status.apply(buf.readUnsignedByte)
      val resp: Response =
         opCode match {
            case PutResponse => new Response(opCode, id, status)
            case GetResponse => {
               val value = {
                  status match {
                     case Success => buf.readRangedBytes
                     case _ => null
                  }
               }
               new RetrievalResponse(opCode, id, status, value)
            }
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
      answer.take
   }

}

private class Op(val code: Byte,
                 val cacheName: String,
                 val key: Array[Byte],
                 val lifespan: Int,
                 val maxIdle: Int,
                 val value: Array[Byte],
                 val flags: Set[Flag])