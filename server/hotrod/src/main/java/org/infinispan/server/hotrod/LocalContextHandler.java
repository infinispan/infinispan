package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.HotRodOperation.PING;
import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.multimap.MultimapCacheDecodeContext;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than the
 * decoder.
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
         if (cdc.header.op.isMultimap())
            realChannelReadMultimap(ctx, cdc, msg, new MultimapCacheDecodeContext(cdc.cache, cdc));
         else
            realChannelRead(ctx, msg, cdc);
      } else {
         super.channelRead(ctx, msg);
      }
   }

   private void realChannelReadMultimap(ChannelHandlerContext ctx, CacheDecodeContext cdc, Object msg, MultimapCacheDecodeContext mcc) throws Exception {
      HotRodHeader h = cdc.header;
      switch (h.op) {
         case CONTAINS_KEY_MULTIMAP:
            writeResponse(cdc, ctx.channel(), mcc.containsKey());
            break;
         case GET_MULTIMAP:
            writeResponse(cdc, ctx.channel(), mcc.get());
            break;
         case GET_MULTIMAP_WITH_METADATA:
            writeResponse(cdc, ctx.channel(), mcc.getWithMetadata());
            break;
         default:
            super.channelRead(ctx, msg);
      }
   }

   private void realChannelRead(ChannelHandlerContext ctx, Object msg, CacheDecodeContext cdc) throws Exception {
      HotRodHeader h = cdc.header;
      switch (h.op) {
         case CONTAINS_KEY:
            writeResponse(cdc, ctx.channel(), cdc.containsKey());
            break;
         case GET:
         case GET_WITH_VERSION:
            writeResponse(cdc, ctx.channel(), cdc.get());
            break;
         case GET_WITH_METADATA:
            writeResponse(cdc, ctx.channel(), cdc.getKeyMetadata());
            break;
         case PING:
            writeResponse(cdc, ctx.channel(), new EmptyResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, PING, OperationStatus.Success, h.topologyId));
            break;
         case STATS:
            writeResponse(cdc, ctx.channel(), cdc.decoder.createStatsResponse(cdc, transport));
            break;
         default:
            super.channelRead(ctx, msg);
      }
   }

}
