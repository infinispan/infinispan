package org.infinispan.server.memcached.binary;

import static org.infinispan.server.memcached.binary.BinaryConstants.MAGIC_RES;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
import org.infinispan.server.memcached.MemcachedInboundAdapter;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.MemcachedStatus;
import org.infinispan.server.memcached.logging.Header;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
abstract class BinaryDecoder extends MemcachedBaseDecoder {

   private final BinaryHeader singleHeader = new BinaryHeader();

   protected BinaryDecoder(MemcachedServer server, Subject subject) {
      super(server, subject, server.getCache().getAdvancedCache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, server.getConfiguration().clientEncoding()));
   }

   // Used by the generated decoder.
   public BinaryHeader acquireHeader() {
      return singleHeader;
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      super.handlerAdded(ctx);
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      metadata.subject(subject);
      metadata.protocolVersion("MCBIN");
   }

   protected MemcachedResponse config(BinaryHeader header, byte[] key) {
      if (log.isTraceEnabled()) {
         log.tracef("CONFIG %s", Util.printArray(key));
      }
      String k = new String(key, StandardCharsets.US_ASCII);
      StringBuilder sb = new StringBuilder();
      if ("cluster".equals(k)) {
         sb.append("1");
         sb.append("\r\n");
         sb.append(server.getHost());
         sb.append('|');
         sb.append(server.getHost());
         sb.append('|');
         sb.append(server.getPort());
         sb.append("\r\n");
      }
      response(header, MemcachedStatus.NO_ERROR, 0, Util.EMPTY_BYTE_ARRAY, sb.toString().getBytes(StandardCharsets.US_ASCII));
      return send(header, CompletableFutures.completedNull());
   }

   protected void response(BinaryHeader header, MemcachedStatus status) {
      response(header, status, Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY);
   }

   protected void response(BinaryHeader header, MemcachedStatus status, byte[] value) {
      response(header, status, Util.EMPTY_BYTE_ARRAY, value);
   }

   protected void response(BinaryHeader header, MemcachedStatus status, byte[] key, byte[] value) {
      int totalLength = key.length + value.length;
      ByteBuf buf = MemcachedInboundAdapter.getAllocator(ctx).acquire(24 + totalLength);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.getCommand().opCode());
      buf.writeShort(key.length); // key length
      buf.writeByte(0); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(totalLength);
      buf.writeInt(header.getOpaque());
      buf.writeLong(header.getCas());
      buf.writeBytes(key);
      buf.writeBytes(value);
   }

   protected void response(BinaryHeader header, MemcachedStatus status, int flags, byte[] key, byte[] value) {
      int totalLength = key.length + value.length + 4;
      ByteBuf buf = MemcachedInboundAdapter.getAllocator(ctx).acquire(24 + totalLength);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.getCommand().opCode());
      buf.writeShort(key.length); // key length
      buf.writeByte(4); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(totalLength);
      buf.writeInt(header.getOpaque());
      buf.writeLong(header.getCas());
      buf.writeInt(flags);
      buf.writeBytes(key);
      buf.writeBytes(value);
   }

   protected void response(BinaryHeader header, MemcachedStatus status, long number) {
      ByteBuf buf = MemcachedInboundAdapter.getAllocator(ctx).acquire(32);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.getCommand().opCode());
      buf.writeShort(0); // key length
      buf.writeByte(0); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(8);
      buf.writeInt(header.getOpaque());
      buf.writeLong(header.getCas());
      buf.writeLong(number);
   }

   protected void response(BinaryHeader header, MemcachedStatus status, Throwable t) {
      response(header, status, t.getMessage().getBytes(StandardCharsets.US_ASCII));
   }

   @Override
   protected MemcachedResponse failedResponse(Header header, Throwable t) {
      return new BinaryResponse(t, header);
   }

   @Override
   public MemcachedResponse send(Header header, CompletionStage<?> response) {
      return new BinaryResponse(response, header, null);
   }

   @Override
   public MemcachedResponse send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener) {
      return new BinaryResponse(response, header, listener);
   }
}
