package org.infinispan.server.hotrod.test

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.infinispan.server.core.transport.netty.NettyChannelBuffer
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import org.infinispan.server.hotrod.{Logging, NoState, VLong, VInt}

/**
 * // TODO: Document this
 *
 * // TODO: Transform to Netty independent code
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Client {
   private var channel: Channel = null

   def connect(host: String, port: Int): Boolean = {
      // Set up.
      val factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)
      val bootstrap: ClientBootstrap = new ClientBootstrap(factory)
      bootstrap.setPipelineFactory(ClientPipelineFactory)
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)
      // Make a new connection.
      val connectFuture = bootstrap.connect(new InetSocketAddress(host, port))
      // Wait until the connection is made successfully.
      channel = connectFuture.awaitUninterruptibly.getChannel
//      // Get the handler instance to retrieve the answer.
//      var handler = channel.getPipeline.getLast.asInstanceOf[ClientHandler]
      connectFuture.isSuccess
   }

   def put(cacheName: String, key: Array[Byte], lifespan: Int, maxIdle: Int, value: Array[Byte]): Boolean = {
      val writeFuture = channel.write(new Store(cacheName, key, lifespan, maxIdle, value))
      writeFuture.awaitUninterruptibly
      writeFuture.isSuccess
   }
   
}

@ChannelPipelineCoverage("all")
private object ClientPipelineFactory extends ChannelPipelineFactory {

   override def getPipeline() = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", Decoder)
      pipeline.addLast("encoder", Encoder)
//      pipeline.addLast("handler", new FactorialClientHandler(count))
      pipeline
   }

}

@ChannelPipelineCoverage("all")
private object Encoder extends OneToOneEncoder {

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any) = {
      val ret =
         msg match {
            case s: Store => {
               val buf = new NettyChannelBuffer(ChannelBuffers.dynamicBuffer)
               buf.writeByte(0xA0.asInstanceOf[Byte]) // magic
               buf.writeByte(41) // version
               buf.writeByte(0x01) // opcode - put
               VInt.write(buf, s.cacheName.length) // cache name length
               buf.writeBytes(s.cacheName.getBytes()) // cache name
               VLong.write(buf, 1) // message id
               VInt.write(buf, 0) // flags
               VInt.write(buf, s.key.length) // key length
               buf.writeBytes(s.key) // key
               VInt.write(buf, s.lifespan) // lifespan
               VInt.write(buf, s.maxIdle) // maxIdle
               VInt.write(buf, s.value.length) // value length
               buf.writeBytes(s.value) // value
               buf.getUnderlyingChannelBuffer
            }
      }
      ret
   }

}

private object Decoder extends ReplayingDecoder[NoState] with Logging {

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buffer: ChannelBuffer, state: NoState) = {
      null
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      error("Error", e.getCause)
   }
}

//private class ClientHandler(val command: Any) extends SimpleChannelUpstreamHandler {
//
//   override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
//      sendCommand(e)
//   }
//
//   override def channelInterestChanged(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
//      sendCommand(e)
//   }
//
//   private def sendCommand(e: ChannelStateEvent) {
//      var channel = e.getChannel
//      channel.write(command)
//   }
//}

private class Store(val cacheName: String, val key: Array[Byte],
                    val lifespan: Int, val maxIdle: Int,
                    val value: Array[Byte])