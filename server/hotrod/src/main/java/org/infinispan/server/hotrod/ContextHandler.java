package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import java.security.PrivilegedActionException;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than the
 * decoder.
 *
 * @author wburns
 * @since 9.0
 */
public class ContextHandler extends SimpleChannelInboundHandler<CacheDecodeContext> {
   private final static Log log = LogFactory.getLog(ContextHandler.class, Log.class);

   private final HotRodServer server;
   private final NettyTransport transport;
   private final Executor executor;
   private final TaskManager taskManager;

   public ContextHandler(HotRodServer server, NettyTransport transport, Executor executor) {
      this.server = server;
      this.transport = transport;
      this.executor = executor;
      this.taskManager = SecurityActions.getGlobalComponentRegistry(server.getCacheManager()).getComponent(TaskManager.class);
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      executor.execute(() -> {
         try {
            realRead(ctx, msg);
         } catch (PrivilegedActionException e) {
            ctx.fireExceptionCaught(e.getCause());
         } catch (Exception e) {
            ctx.fireExceptionCaught(e);
         }
      });
   }

   protected void realRead(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      HotRodHeader h = msg.header;
      switch (h.op) {
         case PUT:
            writeResponse(msg, ctx.channel(), msg.put());
            break;
         case PUT_IF_ABSENT:
            writeResponse(msg, ctx.channel(), msg.putIfAbsent());
            break;
         case REPLACE:
            writeResponse(msg, ctx.channel(), msg.replace());
            break;
         case REPLACE_IF_UNMODIFIED:
            writeResponse(msg, ctx.channel(), msg.replaceIfUnmodified());
            break;
         case CONTAINS_KEY:
            writeResponse(msg, ctx.channel(), msg.containsKey());
            break;
         case GET:
         case GET_WITH_VERSION:
            writeResponse(msg, ctx.channel(), msg.get());
            break;
         case GET_STREAM:
         case GET_WITH_METADATA:
            writeResponse(msg, ctx.channel(), msg.getKeyMetadata());
            break;
         case REMOVE:
            writeResponse(msg, ctx.channel(), msg.remove());
            break;
         case REMOVE_IF_UNMODIFIED:
            writeResponse(msg, ctx.channel(), msg.removeIfUnmodified());
            break;
         case PING:
            writeResponse(msg, ctx.channel(), new EmptyResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, HotRodOperation.PING, OperationStatus.Success, h.topologyId));
            break;
         case STATS:
            writeResponse(msg, ctx.channel(), msg.decoder.createStatsResponse(msg, transport));
            break;
         case CLEAR:
            writeResponse(msg, ctx.channel(), msg.clear());
            break;
         case SIZE:
            writeResponse(msg, ctx.channel(), new SizeResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, h.topologyId, msg.cache.size()));
            break;
         case EXEC:
            ExecRequestContext execContext = (ExecRequestContext) msg.operationDecodeContext;
            Marshaller marshaller;
            if (server.getMarshaller() != null) {
               marshaller = server.getMarshaller();
            } else {
               marshaller = new GenericJBossMarshaller();
            }
            TaskContext taskContext = new TaskContext()
                  .marshaller(marshaller)
                  .cache(msg.cache)
                  .parameters(execContext.getParams())
                  .subject(msg.subject);
            byte[] result = (byte[]) taskManager.runTask(execContext.getName(), taskContext).get();
            writeResponse(msg, ctx.channel(),
                  new ExecResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId,
                        result == null ? new byte[]{} : result));
            break;
         case BULK_GET:
            int size = (int) msg.operationDecodeContext;
            if (CacheDecodeContext.isTrace) {
               log.tracef("About to create bulk response count = %d", size);
            }
            writeResponse(msg, ctx.channel(), new BulkGetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                  h.topologyId, size, msg.cache.entrySet()));
            break;
         case BULK_GET_KEYS:
            int scope = (int) msg.operationDecodeContext;
            if (CacheDecodeContext.isTrace) {
               log.tracef("About to create bulk get keys response scope = %d", scope);
            }
            writeResponse(msg, ctx.channel(), new BulkGetKeysResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                  h.topologyId, scope, msg.cache.keySet().iterator()));
            break;
         case QUERY:
            byte[] queryResult = server.query(msg.cache, (byte[]) msg.operationDecodeContext);
            writeResponse(msg, ctx.channel(),
                  new QueryResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, queryResult));
            break;
         case ADD_CLIENT_LISTENER:
            ClientListenerRequestContext clientContext = (ClientListenerRequestContext) msg.operationDecodeContext;
            server.getClientListenerRegistry().addClientListener(msg.decoder, ctx.channel(), h, clientContext.getListenerId(),
                  msg.cache, clientContext.isIncludeCurrentState(), new KeyValuePair<>(clientContext.getFilterFactoryInfo(),
                        clientContext.getConverterFactoryInfo()), clientContext.isUseRawData(), clientContext.getListenerInterests());
            break;
         case REMOVE_CLIENT_LISTENER:
            byte[] listenerId = (byte[]) msg.operationDecodeContext;
            if (server.getClientListenerRegistry().removeClientListener(listenerId, msg.cache)) {
               writeResponse(msg, ctx.channel(), msg.decoder.createSuccessResponse(h, null));
            } else {
               writeResponse(msg, ctx.channel(), msg.decoder.createNotExecutedResponse(h, null));
            }
            break;
         case ITERATION_START:
            IterationStartRequest iterationStart = (IterationStartRequest) msg.operationDecodeContext;

            Optional<BitSet> optionBitSet;
            if (iterationStart.getOptionBitSet().isPresent()) {
               optionBitSet = Optional.of(BitSet.valueOf(iterationStart.getOptionBitSet().get()));
            } else {
               optionBitSet = Optional.empty();
            }
            String iterationId = server.getIterationManager().start(msg.cache, optionBitSet,
                  iterationStart.getFactory(), iterationStart.getBatch(), iterationStart.isMetadata());
            writeResponse(msg, ctx.channel(), new IterationStartResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, h.topologyId, iterationId));
            break;
         case ITERATION_NEXT:
            iterationId = (String) msg.operationDecodeContext;
            IterableIterationResult iterationResult = server.getIterationManager().next(msg.cache.getName(), iterationId);
            writeResponse(msg, ctx.channel(), new IterationNextResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, h.topologyId, iterationResult));
            break;
         case ITERATION_END:
            iterationId = (String) msg.operationDecodeContext;
            boolean removed = server.getIterationManager().close(msg.cache.getName(), iterationId);
            writeResponse(msg, ctx.channel(), new EmptyResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                  HotRodOperation.ITERATION_END,
                  removed ? OperationStatus.Success : OperationStatus.InvalidIteration, h.topologyId));
            break;
         case PUT_ALL:
            msg.cache.putAll((Map<byte[], byte[]>) msg.operationDecodeContext, msg.buildMetadata());
            writeResponse(msg, ctx.channel(), msg.decoder.createSuccessResponse(h, null));
            break;
         case GET_ALL:
            Map<byte[], byte[]> map = msg.cache.getAll((Set<byte[]>) msg.operationDecodeContext);
            writeResponse(msg, ctx.channel(), new GetAllResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, h.topologyId, map));
            break;
         case PUT_STREAM:
            ByteBuf buf = (ByteBuf) msg.operationDecodeContext;
            try {
               byte[] bytes = new byte[buf.readableBytes()];
               buf.readBytes(bytes);
               msg.operationDecodeContext = bytes;
               long version = msg.params.streamVersion;
               if (version == 0) { // Normal put
                  writeResponse(msg, ctx.channel(), msg.put());
               } else if (version < 0) { // putIfAbsent
                  writeResponse(msg, ctx.channel(), msg.putIfAbsent());
               } else { // versioned replace
                  writeResponse(msg, ctx.channel(), msg.replaceIfUnmodified());
               }
            } finally {
               buf.release();
            }
            break;
         case ROLLBACK_TX:
            writeResponse(msg, ctx.channel(), msg.rollbackTransaction());
            break;
         case PREPARE_TX:
            writeResponse(msg, ctx.channel(), msg.prepareTransaction());
            break;
         case COMMIT_TX:
            writeResponse(msg, ctx.channel(), msg.commitTransaction());
            break;
         default:
            throw new IllegalArgumentException("Unsupported operation invoked: " + msg.header.op);
      }
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      log.tracef("Channel %s became active", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      log.tracef("Channel %s writability changed", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
   }

   @Override
   public boolean acceptInboundMessage(Object msg) throws Exception {
      // Faster than netty matcher
      return msg.getClass() == CacheDecodeContext.class;
   }
}
