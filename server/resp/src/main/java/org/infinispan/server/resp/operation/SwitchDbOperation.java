package org.infinispan.server.resp.operation;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.resp.Resp3Handler;

import io.netty.channel.ChannelHandlerContext;

public class SwitchDbOperation {

   public static void switchDB(Resp3Handler handler, String db, ChannelHandlerContext ctx) {
      Cache<byte[], byte[]> cache = handler.respServer().getCacheManager().getCache(db);
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      handler.setCache(cache.getAdvancedCache().withSubject(metadata.subject()).withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM));
      metadata.cache(cache.getName());
   }

}
