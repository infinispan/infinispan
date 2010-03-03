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

/**
 * // TODO: Document this
 *
 * // TODO: Transform to Netty independent code
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Client {

   def connect(host: String, port: Int) = {
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
      (connectFuture.isSuccess, ch)
   }

   def put(ch: Channel, cacheName: String, key: Array[Byte], lifespan: Int, maxIdle: Int, value: Array[Byte]): Byte = {
      val writeFuture = ch.write(new Store(cacheName, key, lifespan, maxIdle, value))
      writeFuture.awaitUninterruptibly
      assertTrue(writeFuture.isSuccess)
      // Get the handler instance to retrieve the answer.
      var handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse.status.id.byteValue
   }
}

@ChannelPipelineCoverage("all")
private object ClientPipelineFactory extends ChannelPipelineFactory {

   override def getPipeline() = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", Decoder)
      pipeline.addLast("encoder", Encoder)
      pipeline.addLast("handler", new ClientHandler)
      pipeline
   }

}

@ChannelPipelineCoverage("all")
private object Encoder extends OneToOneEncoder {

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any) = {
      val ret =
         msg match {
            case s: Store => {
               val buffer = new NettyChannelBuffer(ChannelBuffers.dynamicBuffer)
               buffer.writeByte(0xA0.asInstanceOf[Byte]) // magic
               buffer.writeByte(41) // version
               buffer.writeByte(0x01) // opcode - put
               buffer.writeUnsignedInt(s.cacheName.length) // cache name length
               buffer.writeBytes(s.cacheName.getBytes()) // cache name
               buffer.writeUnsignedLong(1) // message id
               buffer.writeUnsignedInt(0) // flags
               buffer.writeUnsignedInt(s.key.length) // key length
               buffer.writeBytes(s.key) // key
               buffer.writeUnsignedInt(s.lifespan) // lifespan
               buffer.writeUnsignedInt(s.maxIdle) // maxIdle
               buffer.writeUnsignedInt(s.value.length) // value length
               buffer.writeBytes(s.value) // value
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
      val opCode = buf.readUnsignedByte
      val id = buf.readUnsignedLong
      val status = buf.readUnsignedByte
      new Response(OpCodes.apply(opCode), id, Status.apply(status))
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }
}

@ChannelPipelineCoverage("one")
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

private class Store(val cacheName: String, val key: Array[Byte],
                    val lifespan: Int, val maxIdle: Int,
                    val value: Array[Byte])