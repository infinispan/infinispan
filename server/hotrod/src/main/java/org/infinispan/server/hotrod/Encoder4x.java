package org.infinispan.server.hotrod;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class Encoder4x extends Encoder2x {
   private static final Log log = LogFactory.getLog(Encoder2x.class, Log.class);
   private static final Encoder4x INSTANCE = new Encoder4x();

   public static Encoder4x instance() {
      return INSTANCE;
   }

   @Override
   public ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], byte[]> prev) {
      ByteBuf buf = writeMetadataResponse(header, server, channel, status, prev);
      if (log.isTraceEnabled()) {
         log.tracef("Write response to %s messageId=%d status=%s prev=%s", header.op, header.messageId, status, Util.printArray(prev != null ? prev.getValue() : null));
      }
      return buf;
   }
}
