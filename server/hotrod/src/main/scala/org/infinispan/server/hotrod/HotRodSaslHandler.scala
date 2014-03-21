package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.SaslServerHandler
import javax.security.sasl.SaslServer
import io.netty.buffer.ByteBuf
import javax.security.sasl.SaslException
import OperationStatus._
import io.netty.channel.ChannelHandlerContext
import org.infinispan.server.core.security.AuthorizingCallbackHandler

/**
 * HotRodSaslHandler.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
class HotRodSaslHandler(saslServer: SaslServer, cb: AuthorizingCallbackHandler, h: HotRodHeader, decoder: HotRodDecoder) extends SaslServerHandler[Response](saslServer) {

   override def readHeader(buf: ByteBuf) {
      val header = new HotRodHeader
      decoder.readHeader(buf, header)
      if (header.op != HotRodOperation.AuthRequest) {
         throw new IllegalStateException
      }
   }

   override def newSuccessMessage(ctx: ChannelHandlerContext, challenge: ByteBuf): Response = {
      decoder.subject = cb.getSubjectUserInfo(null).getSubject // FIXME: allow for adding InetAddress and SSL Principals if necessary
      ctx.pipeline().addLast("decoder", decoder)
      new AuthResponse(h.version, h.messageId, h.cacheName, h.clientIntel, challenge.array(), h.topologyId)
   }

   override def newContinueMessage(ctx: ChannelHandlerContext, challenge: ByteBuf): Response = {
      new AuthResponse(h.version, h.messageId, h.cacheName, h.clientIntel, challenge.array(), h.topologyId)
   }

   override def newErrorMessage(ctx: ChannelHandlerContext, e: SaslException):Response = {
      ctx.pipeline().addLast("decoder", decoder)
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel, ServerError, h.topologyId, e.toString)
   }
}
