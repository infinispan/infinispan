package org.infinispan.rest.embedded.netty4;

import java.io.IOException;
import java.io.OutputStream;

import org.infinispan.rest.embedded.netty4.i18n.Messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;

/**
 * Class to help application that are built to write to an
 * OutputStream to chunk the content
 * <p>
 * <pre>
 * {@code
 * DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
 * HttpHeaders.setTransferEncodingChunked(response);
 * response.headers().set(CONTENT_TYPE, "application/octet-stream");
 * //other headers
 * ctx.write(response);
 * // code of the application that use the ChunkOutputStream
 * // Don't forget to close the ChunkOutputStream after use!
 * ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(ChannelFutureListener.CLOSE);
 * }
 * </pre>
 *
 * @author tbussier
 * Temporary fork from RestEasy 3.1.0
 */
public class ChunkOutputStream extends OutputStream {
   final ByteBuf buffer;
   final ChannelHandlerContext ctx;
   final NettyHttpResponse response;

   ChunkOutputStream(NettyHttpResponse response, ChannelHandlerContext ctx, int chunksize) {
      this.response = response;
      if (chunksize < 1) {
         throw new IllegalArgumentException(Messages.MESSAGES.chunkSizeMustBeAtLeastOne());
      }
      this.buffer = Unpooled.buffer(0, chunksize);
      this.ctx = ctx;
   }

   @Override
   public void write(int b) throws IOException {
      if (buffer.maxWritableBytes() < 1) {
         flush();
      }
      buffer.writeByte(b);
   }

   public void reset() {
      if (response.isCommitted()) throw new IllegalStateException(Messages.MESSAGES.responseIsCommitted());
      buffer.clear();
   }

   @Override
   public void close() throws IOException {
      flush();
      super.close();
   }


   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      int dataLengthLeftToWrite = len;
      int dataToWriteOffset = off;
      int spaceLeftInCurrentChunk;
      while ((spaceLeftInCurrentChunk = buffer.maxWritableBytes()) < dataLengthLeftToWrite) {
         buffer.writeBytes(b, dataToWriteOffset, spaceLeftInCurrentChunk);
         dataToWriteOffset = dataToWriteOffset + spaceLeftInCurrentChunk;
         dataLengthLeftToWrite = dataLengthLeftToWrite - spaceLeftInCurrentChunk;
         flush();
      }
      if (dataLengthLeftToWrite > 0) {
         buffer.writeBytes(b, dataToWriteOffset, dataLengthLeftToWrite);
      }
   }

   @Override
   public void flush() throws IOException {
      int readable = buffer.readableBytes();
      if (readable == 0) return;
      if (!response.isCommitted()) response.prepareChunkStream();
      ctx.writeAndFlush(new DefaultHttpContent(buffer.copy()));
      buffer.clear();
      super.flush();
   }

}
