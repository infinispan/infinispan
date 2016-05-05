package org.infinispan.server.hotrod;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.JavaLog;
import org.infinispan.server.hotrod.util.BulkUtil;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import scala.None$;
import scala.Option;
import scala.Tuple2;
import scala.Tuple4;

import java.util.BitSet;
import java.util.Map;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

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
      } else {
         super.channelRead(ctx, msg);
      }
   }
}
