package org.infinispan.hotrod.impl.transport.netty;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;
import static org.infinispan.hotrod.impl.transport.netty.HintedReplayingDecoder.REPLAY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.hotrod.configuration.ClientIntelligence;
import org.infinispan.hotrod.impl.operations.HotRodOperation;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.handler.CacheRequestProcessor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.util.Signal;

abstract class ClientBaseDecoder extends ByteToMessageDecoder {
   protected static final Log log = LogFactory.getLog(ClientBaseDecoder.class);
   public static final Signal DELEGATE = Signal.valueOf(ClientBaseDecoder.class.getName() + ".DELEGATE");
   protected final HeaderDecoder delegate;
   protected final CacheRequestProcessor responseHandler;

   ClientBaseDecoder(HeaderDecoder delegate, CacheRequestProcessor responseHandler) {
      this.delegate = delegate;
      this.responseHandler = responseHandler;
   }


   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      try {
         delegate.exceptionCaught(ctx, cause);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      if (!delegate.isHandlingMessage()) {
         try {
            decode(ctx, in, out);
         } catch (DecoderException de) {
            throw de;
         } catch (Exception e) {
            throw new DecoderException(e);
         }
      }

      if (delegate.isHandlingMessage())
         callInverseDecode(ctx, in, out);
   }

   private void callInverseDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      if (delegate.isHandlingMessage())
         delegate.callDecode(ctx, in, out);

      if (!delegate.isHandlingMessage())
         callDecode(ctx, in, out);
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      delegate.channelInactive(ctx);
   }

   @Override
   public boolean isSharable() {
      return false;
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      delegate.userEventTriggered(ctx, evt);
   }

   public void failoverClientListeners() {
      delegate.failoverClientListeners();
   }

   protected void delegateParsing(ByteBuf buf, long messageId, short opCode, short status) {
      try {
         delegate.replayable.setCumulation(buf);
         delegate.resumeOperation(delegate.replayable, messageId, opCode, status);
      } catch (Signal replay) {
         replay.expect(REPLAY);
         delegate.checkAndAdvance(buf);
      } finally {
         delegate.replayable.setCumulation(null);
      }

      if (delegate.isHandlingMessage()) throw DELEGATE;
   }

   protected boolean operationResponseHasError(long messageId, short opCode) {
      HotRodOperation<?> op = delegate.operation;
      if (op.header().responseCode() != opCode) {
         if (opCode == HotRodConstants.ERROR_RESPONSE) {
            return true;
         }
         throw HOTROD.invalidResponse(new String(op.header().cacheName()), op.header().responseCode(), opCode);
      }

      return false;
   }

   public void removeListener(byte[] id) {
      delegate.removeListener(id);
   }

   protected boolean isHashDistributionAware(long messageId) {
      HotRodOperation<?> op = delegate.operation;
      return op.header().clientIntelligence() == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue();
   }

   public void registerOperation(Channel channel, HotRodOperation<?> op) {
      delegate.registerOperation(channel, op);
   }

   public int registeredOperations() {
      return delegate.registeredOperations();
   }

   public <T extends HotRodOperation<?>> T current() {
      return (T) delegate.operation;
   }

   protected <K, V> Map<K, V> allocMap(int size) {
      return size == 0 ? Collections.emptyMap() : new HashMap<>(size * 4/3, 0.75f);
   }

   protected <T> List<T> allocList(int size) {
      return size == 0 ? Collections.emptyList() : new ArrayList<>(size);
   }

   protected <T> Set<T> allocSet(int size) {
      return size == 0 ? Collections.emptySet() : new HashSet<>(size);
   }
}
