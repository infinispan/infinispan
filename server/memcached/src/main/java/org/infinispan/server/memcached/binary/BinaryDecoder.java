package org.infinispan.server.memcached.binary;

import static org.infinispan.server.memcached.binary.BinaryConstants.MAGIC_RES;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
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

   protected BinaryDecoder(MemcachedServer server, Subject subject) {
      super(server, subject, server.getCache().getAdvancedCache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, server.getConfiguration().clientEncoding()));
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) {
      super.handlerAdded(ctx);
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(channel);
      metadata.subject(subject);
      metadata.protocolVersion("MCBIN");
   }

   protected void config(BinaryHeader header, byte[] key) {
      if (log.isTraceEnabled()) {
         log.tracef("CONFIG %s", Util.printArray(key));
      }
      String k = new String(key, StandardCharsets.US_ASCII);
      StringBuilder sb = new StringBuilder();
      if (k.equals("cluster")) {
         sb.append("1");
         sb.append("\r\n");
         sb.append(server.getHost());
         sb.append('|');
         sb.append(server.getHost());
         sb.append('|');
         sb.append(server.getPort());
         sb.append("\r\n");
      }
      ByteBuf b = response(header, MemcachedStatus.NO_ERROR, 0, Util.EMPTY_BYTE_ARRAY, sb.toString().getBytes(StandardCharsets.US_ASCII));
      send(header, CompletableFuture.completedFuture(b));
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status) {
      return response(header, status, Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY);
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status, byte[] value) {
      return response(header, status, Util.EMPTY_BYTE_ARRAY, value);
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status, byte[] key, byte[] value) {
      int totalLength = key.length + value.length;
      ByteBuf buf = channel.alloc().buffer(24 + totalLength);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.op.opCode());
      buf.writeShort(key.length); // key length
      buf.writeByte(0); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(totalLength);
      buf.writeInt(header.opaque);
      buf.writeLong(header.cas);
      buf.writeBytes(key);
      buf.writeBytes(value);
      return buf;
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status, int flags, byte[] key, byte[] value) {
      int totalLength = key.length + value.length + 4;
      ByteBuf buf = channel.alloc().buffer(24 + totalLength);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.op.opCode());
      buf.writeShort(key.length); // key length
      buf.writeByte(4); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(totalLength);
      buf.writeInt(header.opaque);
      buf.writeLong(header.cas);
      buf.writeInt(flags);
      buf.writeBytes(key);
      buf.writeBytes(value);
      return buf;
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status, long number) {
      ByteBuf buf = channel.alloc().buffer(32);
      buf.writeByte(MAGIC_RES);
      buf.writeByte(header.op.opCode());
      buf.writeShort(0); // key length
      buf.writeByte(0); // extras length
      buf.writeByte(0); // data type
      buf.writeShort(status.getBinary());
      buf.writeInt(8);
      buf.writeInt(header.opaque);
      buf.writeLong(header.cas);
      buf.writeLong(number);
      return buf;
   }

   protected ByteBuf response(BinaryHeader header, MemcachedStatus status, Throwable t) {
      return response(header, status, t.getMessage().getBytes(StandardCharsets.US_ASCII));
   }

   @Override
   public void send(Header header, CompletionStage<?> response) {
      new BinaryResponse(current, channel).queueResponse(accessLogging ? header : null, response);
   }

   @Override
   public void send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener) {
      new BinaryResponse(current, channel).queueResponse(accessLogging ? header : null, response, listener);
   }
}
