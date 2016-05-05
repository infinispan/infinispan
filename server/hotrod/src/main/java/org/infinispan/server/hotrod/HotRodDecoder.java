package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.JavaLog;
import org.infinispan.server.core.transport.ExtendedByteBufJava;
import org.infinispan.server.core.transport.NettyTransport;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Predicate;

/**
 * Decoder that will decode hotrod messages and then send a {@link CacheDecodeContext} down the pipeline.
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodDecoder extends ByteToMessageDecoder {
   private final static JavaLog log = LogFactory.getLog(HotRodDecoder.class, JavaLog.class);

   private final EmbeddedCacheManager cacheManager;
   private final NettyTransport transport;
   private final Predicate<? super String> ignoreCache;
   private final HotRodServer server;

   CacheDecodeContext decodeCtx;
   Throwable previousException;

   private HotRodDecoderState state = HotRodDecoderState.DECODE_HEADER;

   private boolean resetRequested = true;

   public HotRodDecoder(EmbeddedCacheManager cacheManager, NettyTransport transport, HotRodServer server,
           Predicate<? super String> ignoreCache) {
      this.cacheManager = cacheManager;
      this.transport = transport;
      this.ignoreCache = ignoreCache;
      this.server = server;

      this.decodeCtx = new CacheDecodeContext(server);
   }

   public NettyTransport getTransport() {
      return transport;
   }

   void resetNow() {
      decodeCtx = new CacheDecodeContext(server);
      decodeCtx.setHeader(new HotRodHeader());
      state = HotRodDecoderState.DECODE_HEADER;
      resetRequested = false;
   }

   /**
    * Should be called when state is transferred.  This also marks the buffer read
    * position so when we can't read bytes it will be reset to here.
    * @param newState new hotrod decoder state
    * @param buf the byte buffer to mark
    */
   protected void state(HotRodDecoderState newState, ByteBuf buf) {
      buf.markReaderIndex();
      state = newState;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         if (decodeCtx.isTrace()) {
            log.tracef("Decode using instance @%x", System.identityHashCode(this));
         }

         if (resetRequested) {
            resetNow();
         }

         // Mark the index to the beginning, just in case
         in.markReaderIndex();

         switch (state) {
            // These are all fall through cases which means they call to the one below if they needed additional
            // processing
            case DECODE_HEADER:
               if (!decodeHeader(((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().isLoopbackAddress(),
                       in, out)) {
                  break;
               }
               state(HotRodDecoderState.DECODE_KEY, in);
            case DECODE_KEY:
               if (!decodeKey(in, out)) {
                  break;
               }
               state(HotRodDecoderState.DECODE_PARAMETERS, in);
            case DECODE_PARAMETERS:
               if (!decodeParameters(in, out)) {
                  break;
               }
               state(HotRodDecoderState.DECODE_VALUE, in);
            case DECODE_VALUE:
               if (!decodeValue(in, out)) {
                  break;
               }
               state(HotRodDecoderState.DECODE_HEADER, in);
               break;

            // These are terminal cases, meaning they only perform this operation and break
            case DECODE_HEADER_CUSTOM:
               readCustomHeader(in, out);
               break;
            case DECODE_KEY_CUSTOM:
               readCustomKey(in, out);
               break;
            case DECODE_VALUE_CUSTOM:
               readCustomValue(in, out);
               break;
         }
      } catch (Throwable t) {
         previousException = t;
         resetRequested = true;
         // Faster than throwing exception
         ctx.pipeline().fireExceptionCaught(new HotRodException(decodeCtx.createExceptionResponse(t), t));
      }
   }

   boolean decodeHeader(boolean isLoopBack, ByteBuf in, List<Object> out) throws Exception {
      boolean shouldContinue = readHeader(in);
      // If there was nothing present it means we throw this decoding away and start fresh
      if (!shouldContinue) {
         return false;
      }
      HotRodHeader header = decodeCtx.getHeader();
      // Check if this cache can be accessed or not
      if (ignoreCache.test(header.cacheName())) {
         throw new CacheUnavailableException();
      }
      decodeCtx.obtainCache(cacheManager, isLoopBack);
      HotRodOperation op = header.op();
      switch (op.getDecoderRequirements()) {
         case HEADER_CUSTOM:
            state(HotRodDecoderState.DECODE_HEADER_CUSTOM, in);
            readCustomHeader(in, out);
            return false;
         case HEADER:
            // If all we needed was header, we have everything already!
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         default:
            // Continue to key
            return true;
      }
   }

   /**
    * Reads the header and returns whether we should try to continue
    * @param buffer the buffer to read the header from
    * @return whether or not we should continue
    * @throws Exception
    */
   boolean readHeader(ByteBuf buffer) throws Exception {
      AbstractVersionedDecoder decoder = decodeCtx.decoder();
      HotRodHeader header = decodeCtx.header();
      if (decoder == null) {
         if (buffer.readableBytes() < 1) {
            return false;
         }
         short magic = buffer.readUnsignedByte();
         if (magic != Constants$.MODULE$.MAGIC_REQ()) {
            if (previousException == null) {
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic);
            } else {
               log.tracef("Error happened previously, ignoring %d byte until we find the magic number again", magic);
               return false;
            }
         } else {
            previousException = null;
         }

         long messageId = ExtendedByteBufJava.readMaybeVLong(buffer);
         if (messageId == Integer.MIN_VALUE) {
            return false;
         }
         header.messageId_$eq(messageId);
         if (buffer.readableBytes() < 1) {
            buffer.resetReaderIndex();
            return false;
         }
         byte version = (byte) buffer.readUnsignedByte();
         header.version_$eq(version);

         if (Constants$.MODULE$.isVersion2x(version)) {
            decoder = Decoder2x$.MODULE$;
         } else if (Constants$.MODULE$.isVersion1x(version)) {
            decoder = Decoder10$.MODULE$;
         } else {
            throw new UnknownVersionException("Unknown version:" + version, version, messageId);
         }
         decodeCtx.setDecoder(decoder);
         // This way we won't have to reread the decoder related material again
         buffer.markReaderIndex();
      }

      try {
         if (!decoder.readHeader(buffer, header.version(), header.messageId(), header)) {
            return false;
         }
         if (decodeCtx.isTrace()) {
            log.tracef("Decoded header %s", header);
         }
         return true;
      } catch (HotRodUnknownOperationException | SecurityException e) {
         throw e;
      } catch (Exception e) {
         throw new RequestParsingException("Unable to parse header", header.version(), header.messageId(), e);
      }
   }

   private void readCustomHeader(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadHeader(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeKey(ByteBuf in, List<Object> out) {
      HotRodOperation op = decodeCtx.getHeader().op();
      // If we want a single key read that - else we do try for custom read
      if (op.requiresKey()) {
         byte[] bytes = ExtendedByteBufJava.readMaybeRangedBytes(in);
         // If the bytes don't exist then we need to reread
         if (bytes != null) {
            decodeCtx.key_$eq(bytes);
         } else {
            return false;
         }
      }
      switch (op.getDecoderRequirements()) {
         case KEY_CUSTOM:
            state(HotRodDecoderState.DECODE_KEY_CUSTOM, in);
            readCustomKey(in, out);
            return false;
         case KEY:
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         default:
            return true;
      }
   }

   private void readCustomKey(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadKey(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeParameters(ByteBuf in, List<Object> out) {
      RequestParameters params = decodeCtx.decoder().readParameters(decodeCtx.header(), in);
      if (params != null) {
         decodeCtx.params_$eq(params);
         if (decodeCtx.header().op().getDecoderRequirements() == DecoderRequirements.PARAMETERS) {
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         }
         return true;
      } else {
         return false;
      }
   }

   boolean decodeValue(ByteBuf in, List<Object> out) {
      HotRodOperation op = decodeCtx.header().op();
      if (op.requireValue()) {
         int valueLength = decodeCtx.params().valueLength();
         if (in.readableBytes() < valueLength) {
            return false;
         }
         byte[] bytes = new byte[valueLength];
         in.readBytes(bytes);
         decodeCtx.operationDecodeContext_$eq(bytes);
      }
      switch (op.getDecoderRequirements()) {
         case VALUE_CUSTOM:
            state(HotRodDecoderState.DECODE_VALUE_CUSTOM, in);
            readCustomValue(in, out);
            return false;
         case VALUE:
            out.add(decodeCtx);
            resetRequested = true;
            return false;
      }

      return true;
   }

   private void readCustomValue(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadValue(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }
}
