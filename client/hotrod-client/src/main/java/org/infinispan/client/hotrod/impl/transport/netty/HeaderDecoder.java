package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Signal;

public class HeaderDecoder extends HintedReplayingDecoder<HeaderDecoder.State> {
   private static final Log log = LogFactory.getLog(HeaderDecoder.class);
   private static final boolean trace = log.isTraceEnabled();
   // used for HeaderOrEventDecoder, too, as the function is similar
   public static final String NAME = "header-decoder";

   private final Codec codec;
   private final ChannelFactory channelFactory;
   // operations may be registered in any thread, and are removed in event loop thread
   private final ConcurrentMap<Long, HotRodOperation<?>> incomplete = new ConcurrentHashMap<>();
   private volatile boolean closing;

   private HotRodOperation<?> operation;
   private short status;

   public HeaderDecoder(Codec codec, ChannelFactory channelFactory) {
      super(State.READ_MESSAGE_ID);
      this.codec = codec;
      this.channelFactory = channelFactory;
   }

   @Override
   public boolean isSharable() {
      return false;
   }

   public void registerOperation(Channel channel, HotRodOperation<?> operation) {
      if (trace) {
         log.tracef("Registering operation %s(%08X) with id %d on %s",
               operation, System.identityHashCode(operation), operation.header().messageId(), channel);
      }
      if (closing) {
         throw log.noMoreOperationsAllowed();
      }
      HotRodOperation<?> prev = incomplete.put(operation.header().messageId(), operation);
      assert prev == null : "Already registered: " + prev + ", new: " + operation;
      operation.scheduleTimeout(channel.eventLoop());
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         switch (state()) {
            case READ_MESSAGE_ID:
               long messageId = codec.readMessageId(in);
               if (messageId == 0) {
                  // let's read the header even at this stage; it should throw an error and the other throw statement
                  // won't be reached
                  codec.readHeader(in, null, channelFactory, ctx.channel().remoteAddress());
                  throw new IllegalStateException("Should be never reached");
               }
               // we can remove the operation at this point since we'll read no more in this state
               operation = incomplete.remove(messageId);
               // TODO ISPN-8621: events here
               if (operation == null) {
                  throw log.unknownMessageId(messageId);
               }
               if (trace) {
                  log.tracef("Response %d belongs to %s on %s", messageId, operation, ctx.channel());
               }
               checkpoint(State.READ_HEADER);
            case READ_HEADER:
               if (trace) {
                  log.tracef("Decoding header for %s on %s", operation, ctx.channel());
               }
               status = codec.readHeader(in, operation.header(), channelFactory, ctx.channel().remoteAddress());
               checkpoint(State.READ_PAYLOAD);
            case READ_PAYLOAD:
               if (trace) {
                  log.tracef("Decoding payload for %s on %s", operation, ctx.channel());
               }
               operation.acceptResponse(in, status, this);
               checkpoint(State.READ_MESSAGE_ID);
               break;
         }
      } catch (Signal signal) {
         throw signal;
      } catch (Exception e) {
         // If this is server error make sure to restart the state of decoder
         checkpoint(State.READ_MESSAGE_ID);
         throw e;
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (operation != null) {
         operation.exceptionCaught(ctx, cause);
      } else {
         TransportException transportException = log.errorFromUnknownOperation(ctx.channel(), cause, ctx.channel().remoteAddress());
         for (HotRodOperation<?> op: incomplete.values()) {
            try {
               op.exceptionCaught(ctx, transportException);
            } catch (Throwable t) {
               log.errorf(t, "Failed to complete %s", op);
            }
         }
         if (trace) {
            log.tracef(cause, "Requesting %s close due to exception", ctx.channel());
         }
         ctx.close();
      }
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) {
      for (HotRodOperation<?> op: incomplete.values()) {
         try {
            op.channelInactive(ctx.channel());
         } catch (Throwable t) {
            log.errorf(t, "Failed to complete %s", op);
         }
      }
   }

   public CompletableFuture<Void> allCompleteFuture() {
      return CompletableFuture.allOf(incomplete.values().toArray(new CompletableFuture[incomplete.size()]));
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof ChannelPoolCloseEvent) {
         closing = true;
         allCompleteFuture().whenComplete((nil, throwable) -> {
            ctx.channel().close();
         });
      } else if (evt instanceof IdleStateEvent) {
         // If we have incomplete operations this channel is not idle!
         if (!incomplete.isEmpty()) {
            return;
         }
      }
      ctx.fireUserEventTriggered(evt);
   }

   /**
    * {@inheritDoc}
    *
    * Checkpoint is exposed for implementations of {@link HotRodOperation}
    */
   @Override
   public void checkpoint() {
      super.checkpoint();
   }

   public int registeredOperations() {
      return incomplete.size();
   }

   enum State {
      READ_MESSAGE_ID,
      READ_HEADER,
      READ_PAYLOAD
   }
}
