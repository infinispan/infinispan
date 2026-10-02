package org.infinispan.server.core.transport;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Base for the generated protocol decoders, tracking how many bytes the request being parsed has consumed so that a
 * maximum content length can be enforced against it.
 * <p>
 * The count cannot be taken from the reader index alone. A request may be spread over any number of decode
 * invocations, and the cumulation buffer can be compacted or replaced in between, so the progress each invocation
 * makes is folded into a counter instead. {@link #startRequest(ByteBuf)}, which the grammars invoke at the start of
 * every request, clears that counter, so the bytes of a preceding pipelined request are never charged against the
 * request that follows it.
 *
 * @since 16.3
 */
public abstract class RequestLimitDecoder extends ByteToMessageDecoder {
   // Also read by the grammars, which report it when a request is rejected
   protected final int maxContentLength;
   // What bytesAvailable reports when no limit is configured, as the protocols disable the check differently
   private final int unlimited;
   // Reader index the counter below was last brought up to date at
   private int posBefore;
   // Bytes the request being parsed consumed up to posBefore
   private int carriedRequestBytes;
   // The buffer being decoded, only set for the duration of a decode invocation. Only needed by
   // currentRequestBytes(), see there
   private ByteBuf currentBuf;

   protected RequestLimitDecoder(int maxContentLength, int unlimited) {
      this.maxContentLength = maxContentLength;
      this.unlimited = unlimited;
   }

   public final int maxContentLength() {
      return maxContentLength;
   }

   @Override
   protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      currentBuf = in;
      posBefore = in.readerIndex();
      try {
         super.callDecode(ctx, in, out);
      } finally {
         // Whatever the request currently being parsed consumed here has to survive until the next invocation.
         // Re-anchoring keeps the counter usable for as long as this buffer instance is, which covers the
         // decodeLast invocation netty performs on the same buffer right after this one
         carriedRequestBytes += in.readerIndex() - posBefore;
         posBefore = in.readerIndex();
         currentBuf = null;
      }
   }

   /**
    * Invoked when the parser starts reading a new request, so that the bytes of the request preceding it in the
    * pipeline are not charged against it.
    */
   protected void startRequest(ByteBuf buf) {
      carriedRequestBytes = 0;
      posBefore = buf.readerIndex();
   }

   protected int bytesAvailable(ByteBuf buf) {
      if (maxContentLength > 0) {
         return Math.max(maxContentLength - carriedRequestBytes - buf.readerIndex() + posBefore, 0);
      }
      return unlimited;
   }

   /**
    * Number of bytes consumed so far by the request currently being parsed, for reporting only. Prefer
    * {@link #bytesAvailable(ByteBuf)}: this is here for the generated header and error handling methods, which have
    * no buffer parameter. With no buffer being decoded it reports the count as of the last decode invocation.
    */
   public int currentRequestBytes() {
      return currentBuf == null ? carriedRequestBytes : carriedRequestBytes + currentBuf.readerIndex() - posBefore;
   }
}
