package org.infinispan.server.hotrod;

import java.util.List;
import java.util.function.Predicate;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.ExtendedByteBufJava;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Decoder that will decode hotrod messages and then send a {@link CacheDecodeContext} down the pipeline.
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodDecoder extends ByteToMessageDecoder {
   private final static Log log = LogFactory.getLog(HotRodDecoder.class, Log.class);

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
      decodeCtx.header = new HotRodHeader();
      state = HotRodDecoderState.DECODE_HEADER;
      resetRequested = false;
   }

   /**
    * Should be called when state is transferred.  This also marks the buffer read position so when we can't read bytes
    * it will be reset to here.
    *
    * @param newState new hotrod decoder state
    * @param buf      the byte buffer to mark
    */
   protected void state(HotRodDecoderState newState, ByteBuf buf) {
      buf.markReaderIndex();
      state = newState;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         if (CacheDecodeContext.isTrace) {
            log.tracef("Decode buffer %s using instance @%x", dumpHexByteBuf(in), System.identityHashCode(this));
         }

         if (resetRequested) {
            if (CacheDecodeContext.isTrace)
               log.tracef("Reset cached decoder data: %s", decodeCtx);
            resetNow();
         }

         // Mark the index to the beginning, just in case
         in.markReaderIndex();

         switch (state) {
            // These are all fall through cases which means they call to the one below if they needed additional
            // processing
            case DECODE_HEADER:
               if (!decodeHeader(in, out)) {
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
         ctx.pipeline().fireExceptionCaught(new HotRodException(decodeCtx.createExceptionResponse(t), t.getMessage(), t));
      }
   }

   private static String dumpHexByteBuf(ByteBuf in) {
      int maxLength = 32;
      StringBuilder sb = new StringBuilder(maxLength * 2 + 20);
      sb.append('(').append(in.readableBytes()).append(')');
      int startIndex;
      if (in.readableBytes() < maxLength) {
         startIndex = in.readerIndex();
      } else {
         startIndex = in.writerIndex() - maxLength;
         sb.append("...");
      }
      for (int i = startIndex; i < in.writerIndex(); i++) {
         Util.addHexByte(sb, in.getByte(i));
      }
      return sb.toString();
   }

   boolean decodeHeader(ByteBuf in, List<Object> out) throws Exception {
      boolean shouldContinue = readHeader(in);
      // If there was nothing present it means we throw this decoding away and start fresh
      if (!shouldContinue) {
         return false;
      }
      HotRodHeader header = decodeCtx.header;
      // Check if this cache can be accessed or not
      if (ignoreCache.test(header.cacheName)) {
         throw new CacheUnavailableException();
      }
      decodeCtx.obtainCache(cacheManager);
      HotRodOperation op = header.op;
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
    *
    * @param buffer the buffer to read the header from
    * @return whether or not we should continue
    * @throws Exception
    */
   boolean readHeader(ByteBuf buffer) throws Exception {
      VersionedDecoder decoder = decodeCtx.decoder;
      HotRodHeader header = decodeCtx.header;
      if (decoder == null) {
         if (buffer.readableBytes() < 1) {
            buffer.resetReaderIndex();
            return false;
         }
         short magic = buffer.readUnsignedByte();

         if (CacheDecodeContext.isTrace)
            log.tracef("Header magic: %d", magic);

         if (magic != Constants.MAGIC_REQ) {
            if (previousException == null) {
               dumpBuffer("Invalid magic id", buffer);
               throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic);
            } else {
               if (CacheDecodeContext.isTrace) {
                  log.tracef("Error happened previously, ignoring %d byte until we find the magic number again", magic);
               }
               return false;
            }
         } else {
            previousException = null;
         }

         long messageId = ExtendedByteBufJava.readMaybeVLong(buffer);
         if (messageId == Long.MIN_VALUE) {
            return false;
         }

         if (CacheDecodeContext.isTrace) {
            log.tracef("Header message id: %d", messageId);
         }

         header.messageId = messageId;
         if (buffer.readableBytes() < 1) {
            buffer.resetReaderIndex();
            return false;
         }
         byte version = (byte) buffer.readUnsignedByte();
         header.version = version;

         if (CacheDecodeContext.isTrace) {
            log.tracef("Header version: %d", version);
         }

         decoder = HotRodVersion.getDecoder(version);
         if (decoder == null) {
            throw new UnknownVersionException("Unknown version:" + version, version, messageId);
         }
         decodeCtx.decoder = decoder;
         // This way we won't have to reread the decoder related material again
         buffer.markReaderIndex();
      }

      try {
         if (!decoder.readHeader(buffer, header.version, header.messageId, header)) {
            return false;
         }
         if (CacheDecodeContext.isTrace) {
            log.tracef("Decoded header %s", header);
         }
         return true;
      } catch (HotRodUnknownOperationException | SecurityException e) {
         throw e;
      } catch (Exception e) {
         throw new RequestParsingException("Unable to parse header", header.version, header.messageId, e);
      }
   }

   private static void dumpBuffer(String prefix, ByteBuf in) {
      if (log.isTraceEnabled()) {
         String dump = ByteBufUtil.hexDump(in).toUpperCase();
         log.tracef("%s error encountered, the buffer contains: %s", prefix, dump);
      }
   }

   private void readCustomHeader(ByteBuf in, List<Object> out) {
      decodeCtx.decoder.customReadHeader(decodeCtx.header, in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeKey(ByteBuf in, List<Object> out) {
      HotRodOperation op = decodeCtx.header.op;
      // If we want a single key read that - else we do try for custom read
      if (op.requiresKey()) {
         byte[] bytes = ExtendedByteBufJava.readMaybeRangedBytes(in);
         // If the bytes don't exist then we need to reread
         if (bytes != null) {
            if (CacheDecodeContext.isTrace) {
               log.tracef("Body key: %s", Util.printArray(bytes));
            }
            decodeCtx.key = bytes;
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
      decodeCtx.decoder.customReadKey(decodeCtx.header, in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeParameters(ByteBuf in, List<Object> out) {
      CacheDecodeContext.RequestParameters params = decodeCtx.decoder.readParameters(decodeCtx.header, in);
      if (params != null) {
         if (CacheDecodeContext.isTrace) {
            log.tracef("Body parameters: %s", params);
         }
         decodeCtx.params = params;
         if (decodeCtx.header.op.getDecoderRequirements() == DecoderRequirements.PARAMETERS) {
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
      HotRodOperation op = decodeCtx.header.op;
      if (op.requireValue()) {
         int valueLength = decodeCtx.params.valueLength;
         if (in.readableBytes() < valueLength) {
            return false;
         }
         byte[] bytes = new byte[valueLength];
         in.readBytes(bytes);
         decodeCtx.operationDecodeContext = bytes;
         if (CacheDecodeContext.isTrace) {
            log.tracef("Body value: %s", Util.printArray(bytes));
         }
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
      decodeCtx.decoder.customReadValue(decodeCtx.header, in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }
}
