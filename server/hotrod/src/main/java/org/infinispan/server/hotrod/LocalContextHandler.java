package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.security.Security;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than
 * the decoder.
 *
 * @author wburns
 * @since 9.0
 */
public class LocalContextHandler extends ChannelInboundHandlerAdapter {
   private final NettyTransport transport;

   public LocalContextHandler(NettyTransport transport) {
      this.transport = transport;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof CacheDecodeContext) {
         CacheDecodeContext cdc = (CacheDecodeContext) msg;
         Subject subject = ((CacheDecodeContext) msg).getSubject();
         if (subject == null)
            realChannelRead(ctx, msg, cdc);
         else Security.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
            realChannelRead(ctx, msg, cdc);
            return null;
         });
      } else {
         super.channelRead(ctx, msg);
      }
   }

   private void realChannelRead(ChannelHandlerContext ctx, Object msg, CacheDecodeContext cdc) throws Exception {
      HotRodHeader h = cdc.header();
      switch (h.op()) {
         case ContainsKeyRequest:
            writeResponse(cdc, ctx.channel(), cdc.containsKey());
            break;
         case GetRequest:
         case GetWithVersionRequest:
            writeResponse(cdc, ctx.channel(), cdc.get());
            break;
         case GetWithMetadataRequest:
            writeResponse(cdc, ctx.channel(), cdc.getKeyMetadata());
            break;
         case PingRequest:
            writeResponse(cdc, ctx.channel(), new Response(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), OperationResponse.PingResponse(), OperationStatus.Success(), h.topologyId()));
            break;
         case StatsRequest:
            writeResponse(cdc, ctx.channel(), cdc.decoder().createStatsResponse(cdc, transport));
            break;
         default:
            super.channelRead(ctx, msg);
      }
   }

}
