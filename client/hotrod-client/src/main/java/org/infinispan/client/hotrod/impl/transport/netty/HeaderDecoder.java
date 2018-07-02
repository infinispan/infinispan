package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_CREATED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_EXPIRED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_MODIFIED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_REMOVED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.COUNTER_EVENT_RESPONSE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

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
   private final Configuration configuration;
   private final ClientListenerNotifier listenerNotifier;
   // operations may be registered in any thread, and are removed in event loop thread
   private final ConcurrentMap<Long, HotRodOperation<?>> incomplete = new ConcurrentHashMap<>();
   private final List<byte[]> listeners = new ArrayList<>();
   private volatile boolean closing;

   private HotRodOperation<?> operation;
   private short status;
   private short receivedOpCode;

   public HeaderDecoder(Codec codec, ChannelFactory channelFactory, Configuration configuration, ClientListenerNotifier listenerNotifier) {
      super(State.READ_MESSAGE_ID);
      this.codec = codec;
      this.channelFactory = channelFactory;
      this.configuration = configuration;
      this.listenerNotifier = listenerNotifier;
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
               receivedOpCode = codec.readOpCode(in);
               switch (receivedOpCode) {
                  case CACHE_ENTRY_CREATED_EVENT_RESPONSE:
                  case CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
                  case CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
                  case CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
                     if (codec.allowOperationsAndEvents()) {
                        operation = messageId == 0 ? null : incomplete.get(messageId);
                     } else if (incomplete.size() == 1) {
                        operation = incomplete.values().iterator().next();
                        messageId = operation.header().messageId();
                     } else if (incomplete.size() > 1) {
                        throw new IllegalStateException("Too many incomplete operations: " + incomplete);
                     } else {
                        operation = null;
                        messageId = 0;
                     }
                     // The operation may be null even if the messageId was set: the server does not really wait
                     // until all events are sent, only until these are queued. In such case the operation may
                     // complete earlier.
                     if (operation != null && !(operation instanceof AddClientListenerOperation)) {
                        throw log.operationIsNotAddClientListener(messageId, operation.toString());
                     } else if (trace) {
                        log.tracef("This event belongs to %s", operation);
                     }
                     checkpoint(State.READ_CACHE_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
                  case COUNTER_EVENT_RESPONSE:
                     checkpoint(State.READ_COUNTER_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
               }
               if (messageId == 0) {
                  // let's read the header even at this stage; it should throw an error and the other throw statement
                  // won't be reached
                  codec.readHeader(in, receivedOpCode, null, channelFactory, ctx.channel().remoteAddress());
                  throw new IllegalStateException("Should be never reached");
               }
               // we can remove the operation at this point since we'll read no more in this state
               operation = incomplete.remove(messageId);
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
               status = codec.readHeader(in, receivedOpCode, operation.header(), channelFactory, ctx.channel().remoteAddress());
               checkpoint(State.READ_PAYLOAD);
            case READ_PAYLOAD:
               if (trace) {
                  log.tracef("Decoding payload for %s on %s", operation, ctx.channel());
               }
               operation.acceptResponse(in, status, this);
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_CACHE_EVENT:
               if (trace) {
                  log.tracef("Decoding cache event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               AbstractClientEvent cacheEvent;
               try {
                  cacheEvent = codec.readCacheEvent(in, listenerNotifier::getCacheDataFormat,
                        receivedOpCode, configuration.getClassWhiteList(), ctx.channel().remoteAddress());
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  log.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               if (operation != null) {
                  ((AddClientListenerOperation) operation).postponeTimeout(ctx.channel());
               }
               invokeEvent(cacheEvent.getListenerId(), cacheEvent);
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_COUNTER_EVENT:
               if (trace) {
                  log.tracef("Decoding counter event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               HotRodCounterEvent counterEvent;
               try {
                  counterEvent = codec.readCounterEvent(in);
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  log.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               invokeEvent(counterEvent.getListenerId(), counterEvent);
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

   private void invokeEvent(byte[] listenerId, Object cacheEvent) {
      try {
         listenerNotifier.invokeEvent(listenerId, cacheEvent);
      } catch (Exception e) {
         log.unexpectedErrorConsumingEvent(cacheEvent, e);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (operation != null) {
         operation.exceptionCaught(ctx, cause);
      } else {
         TransportException transportException = log.errorFromUnknownOperation(ctx.channel(), cause, ctx.channel().remoteAddress());
         for (HotRodOperation<?> op : incomplete.values()) {
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
      for (HotRodOperation<?> op : incomplete.values()) {
         try {
            op.channelInactive(ctx.channel());
         } catch (Throwable t) {
            log.errorf(t, "Failed to complete %s", op);
         }
      }
      for (byte[] listenerId : listeners) {
         listenerNotifier.failoverClientListener(listenerId);
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

   public void addListener(byte[] listenerId) {
      if (trace) {
         log.tracef("Decoder %08X adding listener %s", hashCode(), Util.printArray(listenerId));
      }
      listeners.add(listenerId);
   }

   // must be called from event loop thread!
   public void removeListener(byte[] listenerId) {
      boolean removed = listeners.removeIf(id -> Arrays.equals(id, listenerId));
      if (trace) {
         log.tracef("Decoder %08X removed? %s listener %s", hashCode(), Boolean.toString(removed), Util.printArray(listenerId));
      }
   }

   enum State {
      READ_MESSAGE_ID,
      READ_HEADER,
      READ_PAYLOAD,
      READ_CACHE_EVENT, READ_COUNTER_EVENT,
   }
}
