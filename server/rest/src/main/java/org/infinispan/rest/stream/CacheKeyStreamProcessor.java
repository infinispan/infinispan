package org.infinispan.rest.stream;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

/**
 * A {@link CacheChunkedStream} that reads from a <code>byte[]</code> and produces a JSON output.
 *
 * @since 10.0
 */
public class CacheKeyStreamProcessor extends CacheChunkedStream<byte[]> {
   public CacheKeyStreamProcessor(Publisher<byte[]> publisher) {
      super(publisher);
   }

   @Override
   public void subscribe(ChannelHandlerContext ctx) {
      publisher.subscribe(new KeySubscriber(ctx, ctx.alloc()));
   }

   static class KeySubscriber extends ByteBufSubscriber<byte[]> {

      protected KeySubscriber(ChannelHandlerContext ctx, ByteBufAllocator allocator) {
         super(ctx, allocator);
      }

      @Override
      void writeItem(byte[] item, ByteBuf pending) {
         String stringified = new String(item, UTF_8);
         byte[] bytesToWrite = stringified.replaceAll("\"", "\\\\\"")
               .getBytes(UTF_8);
         pending.ensureWritable(bytesToWrite.length + 2);
         pending.writeByte('"');
         pending.writeBytes(bytesToWrite);
         pending.writeByte('"');
      }
   }
}
